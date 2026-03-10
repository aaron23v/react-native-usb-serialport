package com.melihyarikkaya.rnserialport;

import android.util.Log;

/**
 * Pseudo-CRC integrity validator for device status packets.
 *
 * USB protocol has no CRC. Structurally valid packets with garbled payload data
 * pass all existing checks. This validator scores multiple known-constrained fields
 * and flags packets that exceed a corruption threshold.
 *
 * The event is always emitted — JS decides whether to drop based on integrityFailed.
 *
 * Validated fields (13 total):
 *
 * Enum/range checks (always applied):
 *   [11] Connection status    — 0 or 1              weight 3
 *   [12] Pulse generated      — 0 or 1              weight 3
 *   [15] Treatment status     — 0, 1, or 2          weight 3
 *   [24] PG type              — 0x00, 0x42, 0x50    weight 3
 *   [8]  Error LSB unused     — bits 2-7 = 0        weight 2
 *   [13-14] MSO               — 0-1000              weight 1
 *   [22-23] Last Pulse Index  — 0-5000              weight 1
 *
 * Note: Coil type [52] and temperature excluded — validated directly at decode layer.
 *
 * Cached on first clean packet (session-stable):
 *   [9]  PG FW Major                                weight 2
 *   [10] PG FW Minor                                weight 2
 *   [30] HMI FW Major                               weight 2
 *   [31] HMI FW Minor                               weight 2
 *   [25-27] PG Serial Number (3 bytes)              weight 2
 *   [6]  Status LSB (hardware identity)             weight 2
 *
 * Drop threshold: score >= 5
 */
public class PacketIntegrityValidator {

    private static final int DROP_THRESHOLD = 5;

    private final String tag;

    // Indicates whether session-stable fields have been cached from first clean packet
    private boolean cached = false;

    // Cached PG firmware version
    private int cachedFwMajor;
    private int cachedFwMinor;

    // Cached HMI firmware version
    private int cachedHmiFwMajor;
    private int cachedHmiFwMinor;

    // Cached PG serial number (3 bytes)
    private int cachedPgSn0;
    private int cachedPgSn1;
    private int cachedPgSn2;

    // Cached Status LSB (PSU type + board BOM revision — hardware identity)
    private int cachedStatusLsb;

    public PacketIntegrityValidator(String logTag) {
        this.tag = logTag;
    }

    /**
     * Result of a packet integrity check.
     */
    public static class Result {
        public final int score;
        public final boolean failed;
        public final String violations;

        public Result(int score, boolean failed, String violations) {
            this.score = score;
            this.failed = failed;
            this.violations = violations;
        }
    }

    /**
     * Validate a raw device-status packet buffer.
     * Caller must ensure packet.length >= 56.
     */
    public Result validate(byte[] packet) {
        int score = 0;
        StringBuilder violations = new StringBuilder();

        // ── Enum / range checks (weight 3) ──

        // Connection status [11]: must be 0 or 1
        int connStatus = packet[11] & 0xFF;
        if (connStatus > 1) {
            score += 3;
            violations.append("connStatus=").append(connStatus).append("; ");
        }

        // Pulse generated [12]: must be 0 or 1
        int pulseGen = packet[12] & 0xFF;
        if (pulseGen > 1) {
            score += 3;
            violations.append("pulseGen=").append(pulseGen).append("; ");
        }

        // Treatment status [15]: must be 0, 1, or 2
        int treatStatus = packet[15] & 0xFF;
        if (treatStatus > 2) {
            score += 3;
            violations.append("treatStatus=").append(treatStatus).append("; ");
        }

        // PG type [24]: must be 0x00, 0x42, or 0x50
        int pgType = packet[24] & 0xFF;
        if (pgType != 0x00 && pgType != 0x42 && pgType != 0x50) {
            score += 3;
            violations.append("pgType=0x").append(String.format("%02X", pgType)).append("; ");
        }

        // ── Unused bit checks (weight 2) ──

        // Error LSB [8]: bits 2-7 must be 0 (only bits 0-1 used)
        int errorLsb = packet[8] & 0xFF;
        if ((errorLsb & 0xFC) != 0) {
            score += 2;
            violations.append("errorLsbUnused=0x").append(String.format("%02X", errorLsb & 0xFC)).append("; ");
        }

        // ── Session-stable cached fields (weight 2 each) ──

        int fwMajor = packet[9] & 0xFF;
        int fwMinor = packet[10] & 0xFF;
        int hmiFwMajor = packet[30] & 0xFF;
        int hmiFwMinor = packet[31] & 0xFF;
        int pgSn0 = packet[25] & 0xFF;
        int pgSn1 = packet[26] & 0xFF;
        int pgSn2 = packet[27] & 0xFF;
        int statusLsb = packet[6] & 0xFF;

        if (!cached) {
            // First clean packet — cache all session-stable fields
            if (score == 0) {
                cachedFwMajor = fwMajor;
                cachedFwMinor = fwMinor;
                cachedHmiFwMajor = hmiFwMajor;
                cachedHmiFwMinor = hmiFwMinor;
                cachedPgSn0 = pgSn0;
                cachedPgSn1 = pgSn1;
                cachedPgSn2 = pgSn2;
                cachedStatusLsb = statusLsb;
                cached = true;
            }
        } else {
            // PG Firmware [9-10]
            if (fwMajor != cachedFwMajor) {
                score += 2;
                violations.append("fwMajor=").append(fwMajor).append("!=").append(cachedFwMajor).append("; ");
            }
            if (fwMinor != cachedFwMinor) {
                score += 2;
                violations.append("fwMinor=").append(fwMinor).append("!=").append(cachedFwMinor).append("; ");
            }

            // HMI Firmware [30-31]
            if (hmiFwMajor != cachedHmiFwMajor) {
                score += 2;
                violations.append("hmiFwMajor=").append(hmiFwMajor).append("!=").append(cachedHmiFwMajor).append("; ");
            }
            if (hmiFwMinor != cachedHmiFwMinor) {
                score += 2;
                violations.append("hmiFwMinor=").append(hmiFwMinor).append("!=").append(cachedHmiFwMinor).append("; ");
            }

            // PG Serial Number [25-27]
            if (pgSn0 != cachedPgSn0 || pgSn1 != cachedPgSn1 || pgSn2 != cachedPgSn2) {
                score += 2;
                violations.append("pgSn=")
                    .append(String.format("%02X%02X%02X", pgSn0, pgSn1, pgSn2))
                    .append("!=")
                    .append(String.format("%02X%02X%02X", cachedPgSn0, cachedPgSn1, cachedPgSn2))
                    .append("; ");
            }

            // Status LSB [6] — PSU type (bits 0-1) + board BOM revision (bits 2-3) = hardware identity
            if (statusLsb != cachedStatusLsb) {
                score += 2;
                violations.append("statusLsb=0x").append(String.format("%02X", statusLsb))
                    .append("!=0x").append(String.format("%02X", cachedStatusLsb))
                    .append("; ");
            }
        }

        // ── Range checks (weight 1) ──

        // MSO [13-14]: 0-1000 (two-byte big-endian, divided by 10)
        int mso = twoBytes(packet[13], packet[14]) / 10;
        if (mso < 0 || mso > 1000) {
            score += 1;
            violations.append("mso=").append(mso).append("; ");
        }

        // Last Pulse Index [22-23]: 0-5000
        int lastPulseIdx = twoBytes(packet[22], packet[23]);
        if (lastPulseIdx < 0 || lastPulseIdx > 5000) {
            score += 1;
            violations.append("lastPulseIdx=").append(lastPulseIdx).append("; ");
        }

        String violationStr = violations.length() > 0 ? violations.toString().trim() : "";
        boolean failed = score >= DROP_THRESHOLD;

        if (failed) {
            Log.w(tag, "INTEGRITY FAIL score=" + score + " violations=[" + violationStr + "]");
        }

        return new Result(score, failed, violationStr);
    }

    /** Convert two bytes (high, low) to unsigned int — mirrors RNSerialportModule.convertTwoBytes */
    private static int twoBytes(byte high, byte low) {
        return ((high & 0xFF) << 8) | (low & 0xFF);
    }
}
