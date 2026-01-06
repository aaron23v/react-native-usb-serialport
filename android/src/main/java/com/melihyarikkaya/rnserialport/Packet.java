package com.melihyarikkaya.rnserialport;

import java.util.Arrays;

/**
 * Immutable packet data object representing a complete TMS device protocol packet.
 *
 * Packet Structure:
 * - Byte 0: Start byte (0x66 or 0xAA)
 * - Bytes 1-3: 24-bit location/address (big-endian)
 * - Byte 4: Data length (0-255)
 * - Bytes 5+: Data payload
 *
 * Thread-safe due to immutability.
 */
public final class Packet {
    private final byte startByte;
    private final int location;
    private final byte[] data;
    private final byte originalSizeByte; // Store original size byte from hardware

    /**
     * Construct a packet with validated data.
     *
     * @param startByte Protocol start byte (0x66 or 0xAA)
     * @param location 24-bit memory location (0 to 16,777,215)
     * @param data Packet payload (defensive copy made)
     */
    public Packet(byte startByte, int location, byte[] data) {
        this(startByte, location, data, (byte) data.length);
    }

    /**
     * Construct a packet with explicit original size byte.
     * Use this constructor for ACK packets where originalSizeByte differs from data.length.
     *
     * @param startByte Protocol start byte (0x66 or 0xAA)
     * @param location 24-bit memory location (0 to 16,777,215)
     * @param data Packet payload (defensive copy made)
     * @param originalSizeByte Original size byte from hardware (for ACK packets)
     */
    public Packet(byte startByte, int location, byte[] data, byte originalSizeByte) {
        if (data == null) {
            throw new IllegalArgumentException("Packet data cannot be null");
        }
        if (location < 0 || location > 0xFFFFFF) {
            throw new IllegalArgumentException("Location must be 24-bit value: " + location);
        }

        this.startByte = startByte;
        this.location = location;
        this.data = Arrays.copyOf(data, data.length); // Defensive copy for immutability
        this.originalSizeByte = originalSizeByte;
    }

    /**
     * Get packet start byte.
     */
    public byte getStartByte() {
        return startByte;
    }

    /**
     * Get 24-bit location/address field.
     */
    public int getLocation() {
        return location;
    }

    /**
     * Get packet data payload (defensive copy).
     */
    public byte[] getData() {
        return Arrays.copyOf(data, data.length);
    }

    /**
     * Get data length.
     */
    public int getDataLength() {
        return data.length;
    }

    /**
     * Get total packet size (header + data).
     */
    public int getTotalSize() {
        return 5 + data.length; // 5-byte header + data
    }

    /**
     * Get complete packet as byte array (header + data).
     * Uses originalSizeByte for byte 4 to preserve hardware ACK format.
     */
    public byte[] toByteArray() {
        byte[] packet = new byte[getTotalSize()];
        packet[0] = startByte;
        packet[1] = (byte) ((location >> 16) & 0xFF);
        packet[2] = (byte) ((location >> 8) & 0xFF);
        packet[3] = (byte) (location & 0xFF);
        packet[4] = originalSizeByte; // Use original size byte, not data.length
        System.arraycopy(data, 0, packet, 5, data.length);
        return packet;
    }

    /**
     * Check if this is a status packet (location = 0).
     */
    public boolean isStatusPacket() {
        return location == 0;
    }

    /**
     * Check if this is a pulse data packet (location >= 2200).
     */
    public boolean isPulseDataPacket() {
        return location >= 2200;
    }

    /**
     * Check if this is a sequence response packet (location = 2103).
     */
    public boolean isSequenceResponsePacket() {
        return location == 2103;
    }

    @Override
    public String toString() {
        return String.format("Packet[start=0x%02X, location=%d, dataLen=%d, totalSize=%d]",
            startByte & 0xFF, location, data.length, getTotalSize());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Packet packet = (Packet) o;
        return startByte == packet.startByte &&
               location == packet.location &&
               Arrays.equals(data, packet.data);
    }

    @Override
    public int hashCode() {
        int result = startByte;
        result = 31 * result + location;
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
