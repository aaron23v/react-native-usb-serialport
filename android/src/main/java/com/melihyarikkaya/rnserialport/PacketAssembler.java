package com.melihyarikkaya.rnserialport;

import android.util.Log;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REDESIGNED PacketAssembler - Fragmentation-First Architecture
 *
 * Core Principles:
 * 1. USB serial streams are CLEAN - no garbage, only fragmentation
 * 2. TRUST buffer contents unless strict validation fails
 * 3. NEVER scan deep into buffer looking for headers (causes false positives)
 * 4. Use MULTI-FIELD validation to confirm valid packets
 * 5. Discard data ONLY when validation definitively fails
 *
 * Packet Structure:
 * - Byte 0: Header (0x66=read response, 0xAA=write ack)
 * - Bytes 1-3: Location (24-bit memory address)
 * - Byte 4: Size (data payload length)
 * - Bytes 5+: Data payload
 *
 * Known Packet Types:
 * - Device Status: 0x66, location=1-399, size=65
 * - Pulse Data: 0x66, location>=2200, size=238 (7 pulses × 34 bytes)
 * - Write ACK: 0xAA, location=varies, size=IGNORED (firmware bug, always 5 bytes)
 *
 * FIX: Prevents false positive header detection that was causing batch data loss.
 * Previous issue: 0x66 bytes in pulse payload were mistaken for packet headers,
 * causing valid data to be discarded as "garbage" (losing 7 pulses per occurrence).
 */
public class PacketAssembler {
    private static final String TAG = "PacketAssembler";
    private static final byte HEADER_READ = (byte) 0x66;  // 102
    private static final byte HEADER_ACK = (byte) 0xAA;   // 170
    private static final int BUFFER_SIZE = 8192;
    private static final int HEADER_SIZE = 5;

    // Packet validation constants
    private static final int PULSE_REGION_START = 2200;
    private static final int PULSE_SIZE = 34;
    private static final int MAX_PULSES_PER_PACKET = 7;
    private static final int PULSE_PACKET_SIZE = MAX_PULSES_PER_PACKET * PULSE_SIZE; // 238
    private static final int DEVICE_STATUS_SIZE = 65;
    private static final int MAX_VALID_LOCATION = 180000; // Upper bound for sanity check

    private final byte[] buffer = new byte[BUFFER_SIZE];
    private int size = 0;

    // Statistics for debugging and health monitoring
    private int totalPacketsExtracted = 0;
    private int totalBytesDiscarded = 0;
    private int fragmentationCount = 0;
    private long lastHealthCheck = 0;
    private static final long HEALTH_CHECK_INTERVAL = 10000; // 10 seconds
    private final Map<String, Integer> validationFailures = new HashMap<>();

    /**
     * Feed new bytes into the assembler and extract complete packets.
     * Handles fragmentation gracefully without aggressive garbage removal.
     */
    public synchronized List<Packet> feedBytes(byte[] newBytes) {
        if (newBytes == null || newBytes.length == 0) {
            return new ArrayList<>();
        }

        // Buffer overflow protection - preserve as much as possible
        if (size + newBytes.length > BUFFER_SIZE) {
            Log.e(TAG, "⚠️ BUFFER OVERFLOW: Have " + size + " bytes, receiving " +
                  newBytes.length + " more (limit: " + BUFFER_SIZE + ")");

            // Try to preserve the most recent complete packet boundary
            int preserveFrom = findLastCompletePacketEnd();
            if (preserveFrom > 0 && preserveFrom < size) {
                int bytesToDiscard = preserveFrom;
                Log.w(TAG, "🔧 RECOVERY: Discarding " + bytesToDiscard +
                      " old bytes, preserving " + (size - bytesToDiscard));
                System.arraycopy(buffer, preserveFrom, buffer, 0, size - preserveFrom);
                size -= bytesToDiscard;
                totalBytesDiscarded += bytesToDiscard;
            } else {
                // Last resort: discard oldest half of buffer
                int halfSize = size / 2;
                Log.e(TAG, "🚨 EMERGENCY: Discarding oldest " + halfSize + " bytes");
                System.arraycopy(buffer, halfSize, buffer, 0, size - halfSize);
                size -= halfSize;
                totalBytesDiscarded += halfSize;
            }

            // Recheck if there's space now
            if (size + newBytes.length > BUFFER_SIZE) {
                Log.e(TAG, "❌ CRITICAL: Still no space after recovery, clearing entire buffer");
                totalBytesDiscarded += size;
                size = 0;
            }
        }

        // Append new bytes
        System.arraycopy(newBytes, 0, buffer, size, newBytes.length);
        size += newBytes.length;

        if (size < HEADER_SIZE) {
            fragmentationCount++;
            Log.d(TAG, "⏳ FRAGMENT: " + size + "/" + HEADER_SIZE + " bytes (waiting for header)");
        }

        // Extract all complete packets
        List<Packet> extracted = new ArrayList<>();

        while (true) {
            PacketValidation validation = validatePacketAtStart();

            if (validation.status == ValidationStatus.VALID_COMPLETE) {
                // Extract the complete packet
                Packet packet = extractPacket(validation);
                extracted.add(packet);
                totalPacketsExtracted++;

                Log.d(TAG, "✅ EXTRACTED #" + totalPacketsExtracted + ": " +
                      validation.packetType + ", Location=0x" + String.format("%06X", packet.getLocation()) +
                      " (" + packet.getLocation() + "), Size=" + packet.getTotalSize() + " bytes");

            } else if (validation.status == ValidationStatus.VALID_INCOMPLETE) {
                // Valid header, waiting for rest of packet
                fragmentationCount++;
                Log.d(TAG, "⏳ FRAGMENT: " + validation.packetType + " at start, have " + size + "/" +
                      validation.expectedTotalSize + " bytes");
                break; // Wait for more data

            } else if (validation.status == ValidationStatus.INVALID_HEADER) {
                // Byte 0 is not a valid header
                handleInvalidHeader();

            } else if (validation.status == ValidationStatus.INVALID_STRUCTURE) {
                // Header looks valid but structure validation failed
                handleInvalidStructure(validation);

            } else {
                // Insufficient data for validation
                fragmentationCount++;
                Log.d(TAG, "⏳ FRAGMENT: " + size + " bytes (need " + HEADER_SIZE + " for header validation)");
                break;
            }
        }

        // Periodic health check
        long now = System.currentTimeMillis();
        if (now - lastHealthCheck > HEALTH_CHECK_INTERVAL) {
            logHealthMetrics();
            lastHealthCheck = now;
        }

        return extracted;
    }

    /**
     * Validate packet structure at buffer start using MULTI-FIELD validation.
     * This prevents false positives from embedded header bytes in payload.
     *
     * KEY FIX: Validates header + location + size together to prevent 0x66/0xAA bytes
     * in pulse payload from being mistaken for packet headers.
     */
    private PacketValidation validatePacketAtStart() {
        PacketValidation result = new PacketValidation();

        // Need at least header to validate
        if (size < HEADER_SIZE) {
            result.status = ValidationStatus.INSUFFICIENT_DATA;
            return result;
        }

        byte header = buffer[0];
        int location = ((buffer[1] & 0xFF) << 16) |
                      ((buffer[2] & 0xFF) << 8) |
                      (buffer[3] & 0xFF);
        int sizeField = buffer[4] & 0xFF;

        // Check if byte 0 is a valid header
        if (header != HEADER_READ && header != HEADER_ACK) {
            result.status = ValidationStatus.INVALID_HEADER;
            result.failureReason = "Byte 0 is not a valid header: 0x" +
                                   String.format("%02X", header);
            return result;
        }

        // Determine expected size based on packet type
        int expectedDataSize;

        if (header == HEADER_ACK) {
            // FIRMWARE BUG: ACK packets always 5 bytes, size field is unreliable
            expectedDataSize = 0;
            result.packetType = "WRITE_ACK";

        } else if (location >= PULSE_REGION_START) {
            // Pulse data packet - must be multiple of PULSE_SIZE
            if (sizeField != PULSE_PACKET_SIZE) {
                // Allow for partial pulse packets (fewer than 7 pulses)
                if (sizeField % PULSE_SIZE != 0 || sizeField == 0 || sizeField > PULSE_PACKET_SIZE) {
                    result.status = ValidationStatus.INVALID_STRUCTURE;
                    result.failureReason = "Pulse packet size invalid: " + sizeField +
                                          " (must be multiple of " + PULSE_SIZE + ", max " + PULSE_PACKET_SIZE + ")";
                    return result;
                }
            }
            expectedDataSize = sizeField;
            result.packetType = "PULSE_DATA";

        } else if (location >= 1 && location <= 399) {
            // Device status packet
            if (sizeField != DEVICE_STATUS_SIZE) {
                result.status = ValidationStatus.INVALID_STRUCTURE;
                result.failureReason = "Device status size mismatch: " + sizeField +
                                      " (expected " + DEVICE_STATUS_SIZE + ")";
                return result;
            }
            expectedDataSize = sizeField;
            result.packetType = "DEVICE_STATUS";

        } else if (location > MAX_VALID_LOCATION) {
            // Location is way out of bounds - likely not a real packet
            result.status = ValidationStatus.INVALID_STRUCTURE;
            result.failureReason = "Location out of bounds: " + location +
                                  " (max " + MAX_VALID_LOCATION + ")";
            return result;

        } else {
            // Other read response (sequence data, waveform data, etc.)
            expectedDataSize = sizeField;
            result.packetType = "OTHER_READ";
        }

        int totalSize = HEADER_SIZE + expectedDataSize;
        result.expectedTotalSize = totalSize;
        result.location = location;
        result.dataSize = expectedDataSize;

        // Check if we have the complete packet
        if (size >= totalSize) {
            result.status = ValidationStatus.VALID_COMPLETE;
        } else {
            result.status = ValidationStatus.VALID_INCOMPLETE;
        }

        return result;
    }

    /**
     * Extract a validated packet from buffer start.
     */
    private Packet extractPacket(PacketValidation validation) {
        byte header = buffer[0];
        byte originalSizeByte = buffer[4]; // Preserve original size byte from hardware
        byte[] data = new byte[validation.dataSize];

        if (validation.dataSize > 0) {
            System.arraycopy(buffer, HEADER_SIZE, data, 0, validation.dataSize);
        }

        // Use 4-argument constructor to preserve original size byte for ACK packets
        Packet packet = new Packet(header, validation.location, data, originalSizeByte);

        // Remove extracted packet from buffer
        int remaining = size - validation.expectedTotalSize;
        if (remaining > 0) {
            System.arraycopy(buffer, validation.expectedTotalSize, buffer, 0, remaining);
        }
        size = remaining;

        return packet;
    }

    /**
     * Handle invalid header at buffer start.
     * Strategy: Discard ONLY 1 byte and retry (minimal loss approach).
     *
     * FIX: Previously would scan entire buffer and discard hundreds of bytes.
     * Now discards only the single invalid byte.
     */
    private void handleInvalidHeader() {
        byte invalidByte = buffer[0];
        Log.w(TAG, "❌ INVALID HEADER at position 0: 0x" + String.format("%02X", invalidByte) +
              " (" + invalidByte + ") - discarding 1 byte");

        // Track failure
        String reason = "Invalid header: 0x" + String.format("%02X", invalidByte);
        validationFailures.put(reason, validationFailures.getOrDefault(reason, 0) + 1);

        // Discard only this single byte
        if (size > 1) {
            System.arraycopy(buffer, 1, buffer, 0, size - 1);
        }
        size--;
        totalBytesDiscarded++;

        // Check if next byte is a valid header (helps detect sync issues)
        if (size >= 1) {
            byte nextByte = buffer[0];
            if (nextByte == HEADER_READ || nextByte == HEADER_ACK) {
                Log.i(TAG, "✓ RESYNC: Found valid header at next position");
            }
        }
    }

    /**
     * Handle structurally invalid packet (valid header but bad structure).
     * Strategy: Discard the header and retry from next byte.
     *
     * FIX: This catches cases where 0x66/0xAA in payload passes header check
     * but fails location/size validation, preventing data loss.
     */
    private void handleInvalidStructure(PacketValidation validation) {
        Log.w(TAG, "❌ INVALID STRUCTURE: " + validation.failureReason);
        Log.w(TAG, "   Header: 0x" + String.format("%02X", buffer[0]) +
              ", Location: " + validation.location +
              ", Size: " + validation.dataSize);

        // Track failure reason
        validationFailures.put(validation.failureReason,
                              validationFailures.getOrDefault(validation.failureReason, 0) + 1);

        // Log the invalid header bytes for debugging
        if (size >= HEADER_SIZE) {
            StringBuilder headerHex = new StringBuilder();
            for (int i = 0; i < HEADER_SIZE; i++) {
                headerHex.append(String.format("%02X ", buffer[i]));
            }
            Log.w(TAG, "   Header bytes: " + headerHex.toString());
        }

        // Discard only the header (5 bytes) and retry
        int discardCount = Math.min(HEADER_SIZE, size);
        if (size > discardCount) {
            System.arraycopy(buffer, discardCount, buffer, 0, size - discardCount);
        }
        size -= discardCount;
        totalBytesDiscarded += discardCount;

        Log.i(TAG, "🔧 RECOVERY: Discarded " + discardCount + " bytes, " + size + " bytes remaining");

        // Log summary every 10 failures
        int totalFailures = 0;
        for (Integer count : validationFailures.values()) {
            totalFailures += count;
        }
        if (totalFailures % 10 == 0 && totalFailures > 0) {
            Log.w(TAG, "📈 VALIDATION FAILURES SUMMARY (total: " + totalFailures + "):");
            for (Map.Entry<String, Integer> entry : validationFailures.entrySet()) {
                Log.w(TAG, "   " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }

    /**
     * Find the end position of the last complete packet in buffer.
     * Used for buffer overflow recovery.
     */
    private int findLastCompletePacketEnd() {
        int pos = 0;
        int lastCompleteEnd = 0;

        while (pos + HEADER_SIZE <= size) {
            byte header = buffer[pos];

            if (header != HEADER_READ && header != HEADER_ACK) {
                pos++;
                continue;
            }

            int location = ((buffer[pos + 1] & 0xFF) << 16) |
                          ((buffer[pos + 2] & 0xFF) << 8) |
                          (buffer[pos + 3] & 0xFF);
            int dataSize = buffer[pos + 4] & 0xFF;

            if (header == HEADER_ACK) {
                dataSize = 0;
            }

            int totalSize = HEADER_SIZE + dataSize;

            if (pos + totalSize <= size) {
                lastCompleteEnd = pos + totalSize;
                pos += totalSize;
            } else {
                break;
            }
        }

        return lastCompleteEnd;
    }

    /**
     * Log health metrics for monitoring buffer stability.
     * Helps detect issues like high discard rates or frequent validation failures.
     */
    private void logHealthMetrics() {
        float discardRate = totalPacketsExtracted > 0 ?
            (float)totalBytesDiscarded / (totalPacketsExtracted * 243) * 100 : 0;

        Log.i(TAG, "📊 BUFFER HEALTH: " +
              "Packets=" + totalPacketsExtracted +
              ", Fragments=" + fragmentationCount +
              ", Discarded=" + totalBytesDiscarded + " bytes (" +
              String.format("%.2f%%", discardRate) + ")");

        if (discardRate > 5.0) {
            Log.w(TAG, "⚠️ HIGH DISCARD RATE: " + String.format("%.2f%%", discardRate) +
                  " - possible sync issues or data corruption");
        }

        // Log top 3 validation failures
        if (!validationFailures.isEmpty()) {
            Log.i(TAG, "📋 TOP VALIDATION FAILURES:");
            validationFailures.entrySet().stream()
                .sorted((a, b) -> b.getValue().compareTo(a.getValue()))
                .limit(3)
                .forEach(entry -> Log.i(TAG, "   " + entry.getKey() + ": " + entry.getValue()));
        }
    }

    /**
     * Get detailed state information for debugging.
     */
    public synchronized String getStateDebugInfo() {
        if (size == 0) {
            return "EMPTY (packets: " + totalPacketsExtracted +
                   ", fragments: " + fragmentationCount +
                   ", discarded: " + totalBytesDiscarded + " bytes)";
        }

        StringBuilder info = new StringBuilder();
        info.append("BUFFERED: ").append(size).append(" bytes");
        info.append(" | Packets: ").append(totalPacketsExtracted);
        info.append(" | Fragments: ").append(fragmentationCount);
        info.append(" | Discarded: ").append(totalBytesDiscarded).append(" bytes");

        // Show first few bytes for debugging
        if (size >= 5) {
            info.append(" | Header: ");
            for (int i = 0; i < Math.min(5, size); i++) {
                info.append(String.format("%02X ", buffer[i]));
            }
        }

        return info.toString();
    }

    /**
     * Validation result container.
     */
    private static class PacketValidation {
        ValidationStatus status;
        String packetType;
        int location;
        int dataSize;
        int expectedTotalSize;
        String failureReason;
    }

    /**
     * Validation status enum.
     */
    private enum ValidationStatus {
        INSUFFICIENT_DATA,     // Not enough bytes to validate
        VALID_COMPLETE,        // Valid packet, fully received
        VALID_INCOMPLETE,      // Valid packet, waiting for payload
        INVALID_HEADER,        // Byte 0 is not a valid header
        INVALID_STRUCTURE      // Header valid but structure check failed
    }
}
