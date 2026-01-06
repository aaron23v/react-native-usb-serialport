export interface IDevice {
  name: string;
  vendorId: number;
  productId: number;
  class: number;
  subclass: number;
}

export type Devices = Array<IDevice> | null;

export interface IOnReadData {
  payload: string | Array<number>
}
export interface IOnError {
  status: boolean;
  errorCode: number;
  errorMessage: string;
  exceptionErrorMessage?: string;
}
export interface IOnServiceStarted {
  deviceAttached: boolean
}


interface DefinitionsStatic {
  DATA_BITS: {
    DATA_BITS_5: number
    DATA_BITS_6: number;
    DATA_BITS_7: number;
    DATA_BITS_8: number;
  };
  STOP_BITS: {
    STOP_BITS_1: number;
    STOP_BITS_15: number;
    STOP_BITS_2: number;
  };
  PARITIES: {
    PARITY_NONE: number;
    PARITY_ODD: number;
    PARITY_EVEN: number;
    PARITY_MARK: number;
    PARITY_SPACE: number;
  };
  FLOW_CONTROLS: {
    FLOW_CONTROL_OFF: number;
    FLOW_CONTROL_RTS_CTS: number;
    FLOW_CONTROL_DSR_DTR: number;
    FLOW_CONTROL_XON_XOFF: number;
  };
  RETURNED_DATA_TYPES: {
    INTARRAY: number;
    HEXSTRING: number;
  };
  DRIVER_TYPES: {
    AUTO: string,
    CDC: string,
    CH34x: string,
    CP210x: string,
    FTDI: string,
    PL2303: string
  };
}
export var definitions: DefinitionsStatic;

interface ActionsStatic {
  ON_SERVICE_STARTED: string,
  ON_SERVICE_STOPPED: string,
  ON_DEVICE_ATTACHED: string,
  ON_DEVICE_DETACHED: string,
  ON_ERROR: string,
  ON_CONNECTED: string,
  ON_DISCONNECTED: string,
  ON_READ_DATA: string
}
export var actions: ActionsStatic;

type DataBits = 5 | 6 | 7 | 8;
type StopBits = 1 | 2 | 3;
type Parities = 0 | 1 | 2 | 3 | 4;
type FlowControls = 0 | 1 | 2 | 3;
type ReturnedDataTypes = 1 | 2;
type Drivers = "AUTO" | "cdc" | "ch34x" | "cp210x" | "ftdi" | "pl2303";

interface RNSerialportStatic {
  /**
   * Starts the service and Usb listener
   *
   * @memberof RNSerialportStatic
   */
  startUsbService(): void;
  /**
   * Stops the service and Usb listener
   *
   * @memberof RNSerialportStatic
   */
  stopUsbService(): void;

  /**
   * Returns status via Promise
   *
   * @returns {Promise<boolean>}
   * @memberof RNSerialportStatic
   */
  isOpen(deviceName: string): Promise<boolean>

  /**
   * Returns status boolean via Promise
   *
   * @returns {Promise<boolean>}
   * @memberof RNSerialportStatic
   */
  isServiceStarted(): Promise<boolean>

  /**
   * Returns support status
   * 
   * @param {string} deviceName
   * @returns {Promise<boolean>}
   * @memberof RNSerialportStatic
   */
  isSupported(deviceName: string): Promise<boolean>;

  //Begin setter methods

  /**
   * Set the returned data type
   *
   * @param {ReturnedDataTypes} type
   * @memberof RNSerialportStatic
   */
  setReturnedDataType(type: ReturnedDataTypes): void;

  /**
   * Set the interface
   *
   * @param {number} iFace
   * @memberof RNSerialportStatic
   */
  setInterface(iFace: number): void;

  /**
   * Set the data bit
   *
   * @param {DataBits} bit
   * @memberof RNSerialportStatic
   */
  setDataBit(bit: DataBits): void;

  /**
   * Set the stop bit
   *
   * @param {StopBits} bit
   * @memberof RNSerialportStatic
   */
  setStopBit(bit: StopBits): void;

  /**
   * Set the parity
   *
   * @param {Parities} parity
   * @memberof RNSerialportStatic
   */
  setParity(parity: Parities): void;

  /**
   *  Set the flow control
   *
   * @param {FlowControls} control
   * @memberof RNSerialportStatic
   */
  setFlowControl(control: FlowControls): void;

  /**
   * Set the auto connection baudrate
   *
   * @param {number} baudRate
   * @memberof RNSerialportStatic
   */
  setAutoConnectBaudRate(baudRate: number): void;

  /**
   * Set the auto connection status
   *
   * @param {boolean} status
   * @memberof RNSerialportStatic
   */
  setAutoConnect(status: boolean): void;

  /**
   * Set the driver type
   *
   * @param {Drivers} driver
   * @memberof RNSerialportStatic
   */
  setDriver(driver: Drivers): void;

  /**
   * Set whether hardware PG enable is allowed (safety mechanism)
   *
   * @param {boolean} allow Whether to allow hardware PG enable
   * @memberof RNSerialportStatic
   */
  setAllowHardwareEnable(allow: boolean): void;

  /**
   * Set the control mode (screen type) for auto-log collection behavior
   * TREATMENT mode: auto-starts log collection after sequence completion
   * MAPPING/MANUAL mode: no auto-log collection
   *
   * @param {string} deviceName The name of the device
   * @param {string} mode The control mode ("TREATMENT", "MAPPING", or "MANUAL")
   * @memberof RNSerialportStatic
   */
  setControlMode(deviceName: string, mode: string): void;

  //End setter methods

  /**
   * Load the default connection settings
   *
   * @memberof RNSerialportStatic
   */
  loadDefaultConnectionSetting(): void;

  /**
   * Returns the device list via Promise
   *
   * @returns {Promise<Device>}
   * @memberof RNSerialportStatic
   */
  getDeviceList(): Promise<Devices>;

  /**
   * Connect to device with device name and baud rate
   *
   * @param {string} deviceName
   * @param {number} baudRate
   * @memberof RNSerialportStatic
   */
  connectDevice(deviceName: string, baudRate: number): void;

  /**
   * Closes the connection
   *
   * @memberof RNSerialportStatic
   */
  disconnectDevice(deviceName: string): void;

  /**
   * Writes string to port
   *
   * @param {string} data
   * @memberof RNSerialportStatic
   */
  writeString(data: string): void;

  /**
   * Writes bytes to port
   *
   * @param {string} deviceName
   * @param {number[]} data
   * @memberof RNSerialportStatic
   */
  writeBytes(deviceName: string, data: number[]): void;

  /**
   * Writes Base64 string to port
   *
   * @param {string} data
   * @memberof RNSerialportStatic
   */
  writeBase64(data: string): void;

  /**
   * Writes hex string to port
   *
   * @param {string} data
   * @memberof RNSerialportStatic
   */
  writeHexString(data: string): void

  /**
   * Integer array convert to Utf16 string
   *
   * @param {Array<number>} intArray
   * @returns {string}
   * @memberof RNSerialportStatic
   */
  intArrayToUtf16(intArray: Array<number>): string

  /**
   * Hex string convert to Utf16 string
   *
   * @param {string} hex
   * @returns {string}
   * @memberof RNSerialportStatic
   */
  hexToUtf16(hex: string): string

  // ============== Native Queue Methods ==============

  /**
   * Add a command to the back of the queue (normal priority)
   *
   * @param {string} deviceName The name of the device to send the command to
   * @param {number[]} command The command to be added to the queue
   * @param {string} functionCaller Identifier for the calling function
   * @memberof RNSerialportStatic
   */
  addToQueue(deviceName: string, command: number[], functionCaller: string): void;

  /**
   * Add a command to the front of the queue (high priority)
   *
   * @param {string} deviceName The name of the device to send the command to
   * @param {number[]} command The command to be added to the front of the queue
   * @param {string} functionCaller Identifier for the calling function
   * @memberof RNSerialportStatic
   */
  addToQueueFront(deviceName: string, command: number[], functionCaller: string): void;

  /**
   * Add a command to the front of the queue, replacing any existing commands with the same signature
   *
   * @param {string} deviceName The name of the device to send the command to
   * @param {number[]} command The command to be added to the front of the queue
   * @param {string} functionCaller Identifier for the calling function
   * @memberof RNSerialportStatic
   */
  addToQueueFrontReplace(deviceName: string, command: number[], functionCaller: string): void;

  /**
   * Write a command to the device immediately by adding it to the front of the queue
   *
   * @param {string} deviceName The name of the device to send the command to
   * @param {number[]} command The command to be written
   * @param {string} functionCaller Identifier for the calling function
   * @memberof RNSerialportStatic
   */
  writeOnClick(deviceName: string, command: number[], functionCaller: string): void;

  /**
   * Write a command to the device immediately by replacing any existing commands with the same signature
   *
   * @param {string} deviceName The name of the device to send the command to
   * @param {number[]} command The command to be written
   * @param {string} functionCaller Identifier for the calling function
   * @memberof RNSerialportStatic
   */
  writeOnClickReplace(deviceName: string, command: number[], functionCaller: string): void;

  /**
   * Write a read command to the device by adding it to the queue (limited to 2 read commands)
   *
   * @param {string} deviceName The name of the device to send the command to
   * @param {number[]} command The read command to be written
   * @param {string} functionCaller Identifier for the calling function
   * @memberof RNSerialportStatic
   */
  writeReadCommand(deviceName: string, command: number[], functionCaller: string): void;

  /**
   * Clear all commands from the queue
   *
   * @memberof RNSerialportStatic
   */
  clearQueue(): void;

  /**
   * Pause the queue processing
   *
   * @memberof RNSerialportStatic
   */
  pauseQueue(): void;

  /**
   * Resume the queue processing
   *
   * @memberof RNSerialportStatic
   */
  resumeQueue(): void;

  /**
   * Clear the timeout for a given command
   *
   * @param {number[]} command The command array to clear timeout for
   * @memberof RNSerialportStatic
   */
  clearCommandTimeout(command: number[]): void;

  // ============== Heartbeat Methods ==============

  /**
   * Start native heartbeat for the specified device
   * Adds heartbeat commands to queue every 250ms, executed with 25ms intervals like all commands
   *
   * @param {string} deviceName The name of the device to start heartbeat for
   * @memberof RNSerialportStatic
   */
  startHeartbeat(deviceName: string): void;

  /**
   * Stop native heartbeat
   *
   * @memberof RNSerialportStatic
   */
  stopHeartbeat(): void;

  // ============== Native Data Processing Methods ==============

  /**
   * Process native read data with enhanced parsing and structured events
   *
   * @param {string} deviceName The name of the device
   * @param {number[]} rawData The raw data array from device
   * @memberof RNSerialportStatic
   */
  processNativeReadData(deviceName: string, rawData: number[]): void;

  // All read command methods removed - redundant with heartbeat command
  // processNativeHardwarePulses removed - caused duplicate pulse emissions


  // ============== Additional Native Methods ==============

  /**
   * Disconnect all connected devices
   *
   * @memberof RNSerialportStatic
   */
  disconnectAllDevices(): void;

  /**
   * Set native gateway mode for device communication
   *
   * @param {boolean} isNativeGw Whether to enable native gateway mode
   * @memberof RNSerialportStatic
   */
  setIsNativeGateway(isNativeGw: boolean): void;

  /**
   * Enable/disable JS event emission for serial port data in native gateway mode
   *
   * @param {boolean} isJsEvent Whether to emit JS events
   * @memberof RNSerialportStatic
   */
  setIsNativeGatewayJsEventEmitOnSerialportData(isJsEvent: boolean): void;

  /**
   * Map application bus to device name
   *
   * @param {number} appBus Application bus identifier
   * @param {string} deviceName Device name to map
   * @memberof RNSerialportStatic
   */
  appBus2DeviceNamePut(appBus: number, deviceName: string): void;

  /**
   * Start manual log collection for specified device
   * Uses native device state to determine log collection parameters
   *
   * @param {string} deviceName The name of the device to start log collection for
   * @memberof RNSerialportStatic
   */
  startManualLogCollection(deviceName: string): void;

  /**
   * Stop native logging for specified device
   *
   * @param {string} deviceName The name of the device to stop logging for
   * @memberof RNSerialportStatic
   */
  stopNativeLogging(deviceName: string): void;

  /**
   * Acknowledge receipt and processing of a pulse batch from native layer
   * Part of ACK protocol for reliable log collection completion
   *
   * @param {string} deviceName The name of the device
   * @param {number} batchSequence The sequence number of the batch being acknowledged
   * @memberof RNSerialportStatic
   */
  acknowledgePulseBatch(deviceName: string, batchSequence: number): void;

  /**
   * Reset device log buffer (clear treatment log table)
   *
   * @param {string} deviceName The name of the device
   * @memberof RNSerialportStatic
   */
  resetDeviceLog(deviceName: string): void;

}
export var RNSerialport: RNSerialportStatic;
