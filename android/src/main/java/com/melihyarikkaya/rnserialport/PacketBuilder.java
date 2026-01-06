package com.melihyarikkaya.rnserialport;

/**
 * Builder for assembling packets from fragmented USB serial data.
 * Tracks packet assembly state and handles incremental byte addition.
 *
 * NOT thread-safe - caller must synchronize access.
 */
public final class PacketBuilder {
    private static final int HEADER_SIZE = 5;
    private static final int MAX_DATA_LENGTH = 255;
    private static final long TIMEOUT_MS = 5000; // 5 seconds for medical device reliability

    // Header: [start byte][location 3 bytes][length 1 byte]
    private final byte[] header = new byte[HEADER_SIZE];
    private int headerPosition = 0;

    // Data payload
    private byte[] data = null;
    private int dataPosition = 0;
    private int expectedDataLength = -1;

    // Timeout tracking
    private final long creationTime;

    /**
     * Create a new packet builder starting with the given start byte.
     *
     * @param startByte Protocol start byte (0x66 or 0xAA)
     */
    public PacketBuilder(byte startByte) {
        this.header[0] = startByte;
        this.headerPosition = 1; // Start byte already stored
        this.creationTime = System.currentTimeMillis();
    }

    /**
     * Add a header byte (location or length field).
     *
     * @param b Byte to add to header
     * @throws IllegalStateException if header already complete
     */
    public void addHeaderByte(byte b) {
        if (headerPosition >= HEADER_SIZE) {
            throw new IllegalStateException("Header already complete");
        }

        header[headerPosition++] = b;

        // When header complete, parse length and allocate data buffer
        if (headerPosition == HEADER_SIZE) {
            expectedDataLength = header[4] & 0xFF; // Unsigned byte

            if (expectedDataLength > 0) {
                data = new byte[expectedDataLength];
            }
        }
    }

    /**
     * Add a data byte to the payload.
     *
     * @param b Data byte to add
     * @throws IllegalStateException if header not complete or data already full
     */
    public void addDataByte(byte b) {
        if (!isHeaderComplete()) {
            throw new IllegalStateException("Cannot add data before header complete");
        }

        if (dataPosition >= expectedDataLength) {
            throw new IllegalStateException("Data buffer full");
        }

        data[dataPosition++] = b;
    }

    /**
     * Check if header (5 bytes) is complete.
     */
    public boolean isHeaderComplete() {
        return headerPosition == HEADER_SIZE;
    }

    /**
     * Check if entire packet is complete (header + all data bytes received).
     */
    public boolean isComplete() {
        return isHeaderComplete() && dataPosition == expectedDataLength;
    }

    /**
     * Check if packet assembly has timed out.
     */
    public boolean isTimedOut() {
        return System.currentTimeMillis() - creationTime > TIMEOUT_MS;
    }

    /**
     * Get expected data length from parsed header.
     *
     * @return Data length, or -1 if header not complete
     */
    public int getExpectedDataLength() {
        return expectedDataLength;
    }

    /**
     * Get 24-bit location from header bytes 1-3 (big-endian).
     *
     * @return Location value, or -1 if header not complete
     */
    public int getLocation() {
        if (headerPosition < 4) {
            return -1;
        }

        return ((header[1] & 0xFF) << 16) |
               ((header[2] & 0xFF) << 8) |
               (header[3] & 0xFF);
    }

    /**
     * Get total bytes received so far (header + data).
     */
    public int getBytesReceived() {
        return headerPosition + dataPosition;
    }

    /**
     * Validate header after it's complete.
     *
     * @return true if valid, false otherwise
     */
    public boolean validateHeader() {
        if (!isHeaderComplete()) {
            return false;
        }

        // Validate start byte
        byte startByte = header[0];
        if (startByte != 102 && startByte != (byte) 170) {
            return false;
        }

        // Validate length field (already parsed, but check bounds)
        if (expectedDataLength < 0 || expectedDataLength > MAX_DATA_LENGTH) {
            return false;
        }

        // Location field: any 24-bit value is valid (0 to 16,777,215)
        int location = getLocation();
        if (location < 0 || location > 0xFFFFFF) {
            return false;
        }

        return true;
    }

    /**
     * Build the final immutable Packet object.
     *
     * @return Complete packet
     * @throws IllegalStateException if packet not complete
     */
    public Packet build() {
        if (!isComplete()) {
            throw new IllegalStateException("Cannot build incomplete packet");
        }

        // For zero-length packets
        byte[] packetData = (expectedDataLength == 0) ? new byte[0] : data;

        return new Packet(header[0], getLocation(), packetData);
    }

    /**
     * Get debug information about current builder state.
     */
    public String getDebugInfo() {
        return String.format("PacketBuilder[location=%d, received=%d/%d bytes, headerComplete=%s, complete=%s]",
            getLocation(), getBytesReceived(), HEADER_SIZE + expectedDataLength,
            isHeaderComplete(), isComplete());
    }
}
