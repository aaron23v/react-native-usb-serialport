package com.melihyarikkaya.rnserialport;

import android.util.Log;

import com.facebook.react.bridge.ReadableArray;
import com.facebook.react.bridge.ReadableMap;
import com.felhr.usbserial.UsbSerialDevice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Native auto-ramp engine for TMS amplitude control.
 *
 * Loaded once from JS with a per-sequence timeline of amplitude deltas.
 * On each heartbeat, compares hardware treatment timestamp against the next
 * keyframe and writes the new Required Voltage directly to USB serial.
 *
 * No timer thread — piggybacks on the existing heartbeat/device-status loop.
 */
public class AutoRampEngine {

    private static final String TAG = "AutoRampEngine";

    // Write command: address 2097 (Required Voltage Percentage), 2 bytes
    private static final int REQUIRED_VOLTAGE_ADDRESS = 2097;

    // Manual-override detection
    private static final int OVERRIDE_TOLERANCE = 10; // % * 10 => 1.0% deviation
    private static final int SETTLING_HEARTBEATS = 3; // suppress detection after our own write

    // Per-device state
    private final ConcurrentHashMap<String, DeviceRampState> deviceStates = new ConcurrentHashMap<>();

    // Reference to serial ports for direct USB writes
    private final Map<String, UsbSerialDevice> serialPorts;

    public AutoRampEngine(Map<String, UsbSerialDevice> serialPorts) {
        this.serialPorts = serialPorts;
    }

    // ── Data structures ──

    public static class RampKeyframe {
        public final long relativeTimestampMs;
        public final int amplitudeDelta; // delta in amplitude % (can be negative)
        public final int pulseNumber;
        public final int trainNumber;

        public RampKeyframe(long relativeTimestampMs, int amplitudeDelta, int pulseNumber, int trainNumber) {
            this.relativeTimestampMs = relativeTimestampMs;
            this.amplitudeDelta = amplitudeDelta;
            this.pulseNumber = pulseNumber;
            this.trainNumber = trainNumber;
        }
    }

    public static class SequenceTimeline {
        public final int sequenceIndex;
        public final String sequenceId;
        public final List<RampKeyframe> keyframes;

        public SequenceTimeline(int sequenceIndex, String sequenceId, List<RampKeyframe> keyframes) {
            this.sequenceIndex = sequenceIndex;
            this.sequenceId = sequenceId;
            this.keyframes = keyframes;
        }
    }

    private static class DeviceRampState {
        List<SequenceTimeline> timelines = new ArrayList<>();
        int currentSequenceIndex = 0;
        int nextKeyframeIndex = 0;
        int trackedAmplitude = -1; // current amplitude in % * 10 (e.g. 500 = 50.0%)
        int targetMaxTimesTen = 1000; // amplitude ceiling in % * 10; default 100.0%
        int settlingCounter = 0; // heartbeats to wait for hardware to reflect our write
        int lastObservedMso = -1; // last hardware amplitude reading (%), to detect real movement
        int lastTreatmentStatus = -1;
        boolean active = false;
        boolean overridden = false; // a manual adjustment disabled ramp for the session

        void reset() {
            currentSequenceIndex = 0;
            nextKeyframeIndex = 0;
            trackedAmplitude = -1;
            settlingCounter = 0;
            lastObservedMso = -1;
            active = false;
            overridden = false;
        }
    }

    // ── Public API ──

    /**
     * Load timeline data from JS. Called once before treatment starts.
     * Expects ReadableArray of objects:
     * [{ sequenceIndex, sequenceId, timeline: [{ relativeTimestamp, amplitudeDelta, pulseNumber, trainNumber }] }]
     */
    public void loadTimeline(String deviceName, ReadableArray timelineData, double targetMaxPercent) {
        DeviceRampState state = new DeviceRampState();

        // Clamp the ceiling to a valid 0-100% and store as % * 10. A non-positive
        // value means "no app limit provided" -> fall back to full 100.0%.
        int maxTimesTen = (int) Math.round(targetMaxPercent * 10);
        state.targetMaxTimesTen = (maxTimesTen > 0) ? Math.min(maxTimesTen, 1000) : 1000;

        for (int i = 0; i < timelineData.size(); i++) {
            ReadableMap seqMap = timelineData.getMap(i);
            int sequenceIndex = seqMap.getInt("sequenceIndex");
            String sequenceId = seqMap.getString("sequenceId");
            ReadableArray timelineArr = seqMap.getArray("timeline");

            List<RampKeyframe> keyframes = new ArrayList<>();
            for (int j = 0; j < timelineArr.size(); j++) {
                ReadableMap kf = timelineArr.getMap(j);
                long relTs = (long) kf.getDouble("relativeTimestamp");
                int delta = (int) kf.getDouble("amplitudeDelta");
                int pulse = kf.hasKey("pulseNumber") ? (int) kf.getDouble("pulseNumber") : 0;
                int train = kf.hasKey("trainNumber") ? (int) kf.getDouble("trainNumber") : 0;
                keyframes.add(new RampKeyframe(relTs, delta, pulse, train));
            }

            state.timelines.add(new SequenceTimeline(sequenceIndex, sequenceId, keyframes));
            Log.i(TAG, "Loaded sequence " + sequenceIndex + " (" + sequenceId + ") with " + keyframes.size() + " keyframes");
        }

        deviceStates.put(deviceName, state);
        Log.i(TAG, "Timeline loaded for " + deviceName + ": " + state.timelines.size() + " sequences");
    }

    /**
     * Called from device status processing on every heartbeat.
     *
     * @param deviceName      USB device path
     * @param hardwareTimestamp Treatment progress time in ms from hardware (bytes 37-40)
     * @param treatmentStatus  0=stopped, 1=running, 2=paused
     * @param currentMso       Current actual amplitude from hardware (already divided by 10, in %)
     */
    public boolean checkAndApply(String deviceName, long hardwareTimestamp, int treatmentStatus, int currentMso) {
        DeviceRampState state = deviceStates.get(deviceName);
        if (state == null || state.timelines.isEmpty()) {
            return false;
        }

        // Handle treatment status transitions
        if (state.lastTreatmentStatus != treatmentStatus) {
            handleStatusTransition(deviceName, state, state.lastTreatmentStatus, treatmentStatus, currentMso);
            state.lastTreatmentStatus = treatmentStatus;
        }

        // Only process keyframes while treatment is running
        if (treatmentStatus != 1 || !state.active) {
            return false;
        }

        // Manual-override detection: flag only when the hardware amplitude actually
        // MOVES between heartbeats without the engine having just written it (a real
        // knob turn). A persistent offset — e.g. the hardware lagging or never
        // reaching a 0% write — is NOT an override, so it no longer false-triggers.
        if (state.settlingCounter > 0) {
            // Our own write is still propagating; absorb whatever the hardware reads.
            state.settlingCounter--;
        } else if (state.lastObservedMso >= 0
                && Math.abs(currentMso - state.lastObservedMso) * 10 > OVERRIDE_TOLERANCE) {
            Log.i(TAG, "Manual override detected: hw moved " + state.lastObservedMso
                    + "% -> " + currentMso + "% — disabling auto-ramp");
            state.active = false;
            state.overridden = true; // stays disabled for the rest of the session
            state.lastObservedMso = currentMso;
            return true;
        }
        state.lastObservedMso = currentMso;

        // Get current sequence timeline
        if (state.currentSequenceIndex >= state.timelines.size()) {
            return false;
        }
        SequenceTimeline currentTimeline = state.timelines.get(state.currentSequenceIndex);
        List<RampKeyframe> keyframes = currentTimeline.keyframes;

        if (state.nextKeyframeIndex >= keyframes.size()) {
            return false; // All keyframes consumed for this sequence
        }

        // Check if hardware timestamp has passed the next keyframe
        RampKeyframe nextKf = keyframes.get(state.nextKeyframeIndex);
        if (hardwareTimestamp >= nextKf.relativeTimestampMs) {
            // Apply all keyframes that have been passed (in case we skipped some)
            while (state.nextKeyframeIndex < keyframes.size()) {
                RampKeyframe kf = keyframes.get(state.nextKeyframeIndex);
                if (hardwareTimestamp < kf.relativeTimestampMs) {
                    break;
                }

                state.trackedAmplitude += kf.amplitudeDelta * 10; // delta is in %, tracked is in % * 10

                // Clamp: never below 0, never above the app's amplitude limit
                state.trackedAmplitude = Math.max(0, Math.min(state.targetMaxTimesTen, state.trackedAmplitude));

                Log.i(TAG, "Keyframe " + state.nextKeyframeIndex + " applied: delta=" + kf.amplitudeDelta +
                        "%, new amplitude=" + (state.trackedAmplitude / 10.0) + "%" +
                        " (hw_ts=" + hardwareTimestamp + "ms, kf_ts=" + kf.relativeTimestampMs + "ms)");

                state.nextKeyframeIndex++;
            }

            // Write the new amplitude to hardware, then suppress override detection
            // for a few heartbeats while the hardware catches up to our write.
            writeRequiredVoltage(deviceName, state.trackedAmplitude);
            state.settlingCounter = SETTLING_HEARTBEATS;
        }

        return false;
    }

    /**
     * Advance to next sequence. Called from JS when sequence transitions.
     */
    public void advanceSequence(String deviceName) {
        DeviceRampState state = deviceStates.get(deviceName);
        if (state == null) return;

        state.currentSequenceIndex++;
        state.nextKeyframeIndex = 0;

        if (state.currentSequenceIndex < state.timelines.size()) {
            Log.i(TAG, "Advanced to sequence " + state.currentSequenceIndex +
                    " (" + state.timelines.get(state.currentSequenceIndex).sequenceId + ")");
        } else {
            Log.i(TAG, "All sequences completed for " + deviceName);
            state.active = false;
        }
    }

    /**
     * Clear all state for a device.
     */
    public void clear(String deviceName) {
        deviceStates.remove(deviceName);
        Log.i(TAG, "Cleared auto-ramp state for " + deviceName);
    }

    /**
     * Check if auto-ramp is loaded for a device.
     */
    public boolean isLoaded(String deviceName) {
        DeviceRampState state = deviceStates.get(deviceName);
        return state != null && !state.timelines.isEmpty();
    }

    // ── Private helpers ──

    private void handleStatusTransition(String deviceName, DeviceRampState state, int oldStatus, int newStatus, int currentMso) {
        Log.i(TAG, "Treatment status transition: " + oldStatus + " -> " + newStatus + " for " + deviceName);

        if (newStatus == 1) {
            // Treatment started or resumed. Anchor the amplitude on a fresh start,
            // OR on the very first activation even if it arrives as a resume-from-pause
            // (the engine can observe an initial PAUSED status before PLAYING). Without
            // this, trackedAmplitude is never seeded and the ramp starts from 0 instead
            // of the operator's set amplitude.
            if (oldStatus != 2 || state.trackedAmplitude < 0) {
                state.nextKeyframeIndex = 0;
                state.trackedAmplitude = currentMso * 10; // Initialize from hardware's actual amplitude
                state.lastObservedMso = currentMso; // baseline for movement detection
                Log.i(TAG, "Auto-ramp started. Initial amplitude: " + currentMso + "%");
            }
            // A manual override disables ramp for the session; pause/resume must
            // not revive it. Only (re)activate when not overridden.
            if (!state.overridden) {
                state.active = true;
            }
        } else if (newStatus == 2) {
            // Paused — hold position, don't reset
            Log.i(TAG, "Auto-ramp paused");
        } else if (newStatus == 0) {
            // Stopped — reset keyframe pointer for current sequence
            state.nextKeyframeIndex = 0;
            state.active = false;
            Log.i(TAG, "Auto-ramp stopped");
        }
    }

    /**
     * Write Required Voltage Percentage directly to USB serial.
     * Address 2097, 2 bytes, value = amplitude * 10 (e.g. 50.0% = 0x01F4).
     *
     * Write command format: [0xAA, addr_B3, addr_B2, addr_B1, num_bytes, MSB, LSB]
     */
    private void writeRequiredVoltage(String deviceName, int amplitudeTimesTen) {
        UsbSerialDevice serialPort = serialPorts.get(deviceName);
        if (serialPort == null) {
            Log.w(TAG, "Cannot write amplitude - no serial port for " + deviceName);
            return;
        }

        byte[] command = new byte[7];
        command[0] = (byte) 0xAA; // Write command identifier
        command[1] = (byte) ((REQUIRED_VOLTAGE_ADDRESS >> 16) & 0xFF); // Address byte 3 (0x00)
        command[2] = (byte) ((REQUIRED_VOLTAGE_ADDRESS >> 8) & 0xFF);  // Address byte 2 (0x08)
        command[3] = (byte) (REQUIRED_VOLTAGE_ADDRESS & 0xFF);          // Address byte 1 (0x31)
        command[4] = (byte) 2; // Number of bytes to write
        command[5] = (byte) ((amplitudeTimesTen >> 8) & 0xFF); // MSB
        command[6] = (byte) (amplitudeTimesTen & 0xFF);         // LSB

        try {
            serialPort.write(command);
            Log.d(TAG, String.format("AUTO-RAMP TX: amplitude=%.1f%% (0x%04X) to %s",
                    amplitudeTimesTen / 10.0, amplitudeTimesTen, deviceName));
        } catch (Exception e) {
            Log.e(TAG, "Failed to write auto-ramp amplitude: " + e.getMessage(), e);
        }
    }
}
