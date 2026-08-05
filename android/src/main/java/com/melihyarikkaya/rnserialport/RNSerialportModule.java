package com.melihyarikkaya.rnserialport;

import android.app.PendingIntent;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;

import com.facebook.react.bridge.Callback;
import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.Arguments;
import com.facebook.react.bridge.LifecycleEventListener;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.bridge.ReadableArray;
import com.facebook.react.bridge.ReadableMap;
import com.facebook.react.bridge.WritableArray;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.module.annotations.ReactModule;
import com.facebook.react.modules.core.DeviceEventManagerModule;
import com.facebook.react.bridge.WritableNativeArray;

import android.annotation.SuppressLint;
import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkCapabilities;
import android.net.NetworkRequest;
import android.net.Network;
import android.util.Base64;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.text.SimpleDateFormat;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import android.hardware.usb.UsbDevice;
import android.hardware.usb.UsbDeviceConnection;
import android.hardware.usb.UsbManager;

import com.felhr.usbserial.UsbSerialDevice;
import com.felhr.usbserial.UsbSerialInterface;

import com.google.common.primitives.UnsignedBytes;

public class RNSerialportModule extends ReactContextBaseJavaModule implements LifecycleEventListener, TcpReceiverTask.OnDataReceivedListener {
    public static final String TAG = "RNSerialport";
    private static final int N_THREADS = 2;
    private static final int USB_PROCESSING_THREADS = 4; // Dedicated threads for USB packet processing
    public final ReactApplicationContext mReactContext;
    private final ConcurrentHashMap<Integer, TcpSocket> socketMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Network> mNetworkMap = new ConcurrentHashMap<>();
    private final CurrentNetwork currentNetwork = new CurrentNetwork();
    private final ExecutorService executorService = Executors.newFixedThreadPool(N_THREADS);
    // CRITICAL FIX: Dedicated thread pool for USB packet processing to prevent thread starvation
    // and ensure USB read callbacks remain non-blocking during high-frequency data collection
    private final ExecutorService usbProcessingExecutor = Executors.newFixedThreadPool(USB_PROCESSING_THREADS);
    private static final int[] TARGET_PRODUCT_IDS = {0x6015, 0x6001};

    // Native packet buffers for USB data assembly (per-device for parallel processing)
    // FIX #9: Per-device buffers to eliminate serialization bottleneck
    // BOTTLENECK: Single shared buffer forces 4 USB threads to serialize through synchronized method
    // SOLUTION: One buffer per device enables true parallel processing while maintaining thread-safety
    private final Map<String, NativePacketBuffer> devicePacketBuffers = new ConcurrentHashMap<>();

    // Native Serial Queue Infrastructure
    private static class CommandItem implements Comparable<CommandItem> {
        final String deviceName;
        final byte[] command;
        final int priority;
        final long timestamp;
        final String functionCaller;
        final String commandKey;
        int retryCount;

        CommandItem(String deviceName, byte[] command, int priority, String functionCaller, int retryCount) {
            this.deviceName = deviceName;
            this.command = command.clone();
            this.priority = priority;
            this.timestamp = System.currentTimeMillis();
            this.functionCaller = functionCaller;
            this.retryCount = retryCount;
            this.commandKey = generateCommandKey(command);
        }

        @Override
        public int compareTo(CommandItem other) {
            if (this.priority != other.priority) {
                return Integer.compare(other.priority, this.priority);
            }
            return Long.compare(this.timestamp, other.timestamp);
        }

        private String generateCommandKey(byte[] command) {
            // Use first 5 bytes for key generation to match acknowledgment signature
            // This ensures acknowledgments can clear timeouts correctly
            int keyLength = Math.min(command.length, 5);
            byte[] keyBytes = new byte[keyLength];
            System.arraycopy(command, 0, keyBytes, 0, keyLength);
            String key = java.util.Arrays.toString(keyBytes);
            android.util.Log.v(TAG, "🔑 Generated command key: " + key + " from command: " + java.util.Arrays.toString(command));
            return key;
        }
    }

    // Queue Infrastructure - Thread-safe implementation
    private final PriorityBlockingQueue<CommandItem> nativeQueue = new PriorityBlockingQueue<>();
    private volatile boolean isProcessing = false;
    private volatile boolean isPaused = false;

    // Threading - Separate schedulers for queue and USB I/O
    private final ScheduledExecutorService queueScheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService usbWriteExecutor = Executors.newSingleThreadExecutor();

    // Priority Constants
    private static final int PRIORITY_HEARTBEAT = 1;
    private static final int PRIORITY_NORMAL = 2;
    private static final int PRIORITY_FRONT = 3;
    private static final int PRIORITY_REPLACE = 4;

    // Timing Constants
    private static final int COMMAND_INTERVAL_MS = 50;  // 50ms between ALL commands
    private static final int LOG_COMMAND_INTERVAL_MS = 200;  // 200ms between log read commands (device needs time to respond)
    private static final int USB_WRITE_TIMEOUT_MS = 40;  // Write timeout must be < COMMAND_INTERVAL_MS
    private static final int USB_READ_TIMEOUT_MS = 100;  // Industry standard read timeout
    private static final int RETRY_TIMEOUT_MS = 1000;  // Regular command timeout
    private static final int MAX_RETRIES = 3;
    private static final int MAX_QUEUE_SIZE = 10;
    private static final int HEARTBEAT_INTERVAL_MS = 250;  // Heartbeat every 250ms
    private static final int CLEANUP_TIMEOUT_SECONDS = 5;  // Cleanup timeout in seconds

    // Log Collection Timeout Constants
    private static final int INACTIVITY_TIMEOUT_MS = 10000;  // 10 seconds after last ACK received
    private static final int ABSOLUTE_TIMEOUT_MS = 120000;   // 2 minutes absolute safety timeout

    // Heartbeat Management
    private volatile String heartbeatDevice = null;
    private volatile ScheduledFuture<?> heartbeatTimer = null;
    private static final byte[] HEARTBEAT_COMMAND = {0x66, 0x00, 0x00, 0x00, 0x35}; // Status read, request 53 bytes (was 51) to cover RevG resistor temp (doc 51) + fan speed (doc 52). Assumes firmware honors the requested count — verify on hardware via the [RX] hex log that the response is 58 bytes.
    // Removed LIGHTWEIGHT_HEARTBEAT_COMMAND - no longer needed with priority queue approach

    // Retry Management
    private final ConcurrentHashMap<String, ScheduledFuture<?>> retryTimeouts = new ConcurrentHashMap<>();

    // Temperature Smoothing - EMA filter to reduce noise in calculatedTemperature
    private final ConcurrentHashMap<String, Double> temperatureCache = new ConcurrentHashMap<>();
    private static final double TEMPERATURE_SMOOTHING_FACTOR = 0.3; // 30% new value, 70% previous (aggressive smoothing)

    // Packet integrity validator for detecting garbled USB data
    private final PacketIntegrityValidator packetIntegrityValidator = new PacketIntegrityValidator(TAG);
    private final TimestampValidator timestampValidator = new TimestampValidator();

    // CRC-16 (protocol RevG, section 1.3.5), gated on PG firmware version (read from the
    // device-status packet bytes 9=major/10=minor, cached in PacketIntegrityValidator).
    // Below the threshold, behaviour is byte-identical to before.
    //
    // Per the AMPA tester, CRC is present on every message EXCEPT the write ACK:
    //   - outgoing read request  (0x66): CRC appended      (writeSerialportBytes)
    //   - outgoing write request (0xAA): CRC appended      (writeSerialportBytes)
    //   - incoming read response (0x66): CRC validated     (processPacketDirectly)
    //   - incoming write ACK     (0xAA): NO CRC            (plain 5-byte ack)
    //
    // EDITABLE THRESHOLD: CRC is active from PG firmware V0.20 onward (V20 and above).
    private static final int CRC_MIN_FW_MAJOR = 0;
    private static final int CRC_MIN_FW_MINOR = 20;
    // Length of the CRC-16 trailer appended to commands/responses when CRC is enabled.
    private static final int CRC_TRAILER_LEN = 2;
    // Per-device CRC enablement: null = undecided (fw not yet known), TRUE/FALSE once decided.
    private final Map<String, Boolean> crcEnabledByDevice = new ConcurrentHashMap<>();

    // Train skip-direction (protocol RevG, location 2118) skip-backward was unreliable on
    // older PG firmware; the Tablet app kept the "Train back" button force-disabled until now.
    // EDITABLE THRESHOLD: Train back is supported from PG firmware V0.20 onward (V20 and above).
    private static final int TRAIN_BACK_MIN_FW_MAJOR = 0;
    private static final int TRAIN_BACK_MIN_FW_MINOR = 20;

    // Special Command Signatures
    private static final byte[] READ_COMMAND_SIG = {102, 0, 0, 0, 53};
    private static final byte[] RESET_COMMAND_SIG = {(byte)170, 0, 8, 55, 15, 23, 112, 9, 39, (byte)192, 23, 112, 9, 39, (byte)192, 23, 112, 9, 39, (byte)192};
    private static final byte[] PG_DISABLE_COMMAND = {(byte)0xAA, 0x00, 0x08, 0x36, 0x01, 0x00}; // Disable PG at location 2102
    private static final byte[] CAMERA_ON_COMMAND = {(byte)0xAA, 0x00, 0x08, 0x35, 0x01, 0x00};  // Location 2101 (0x0835), value 0 = Camera ON
    private static final byte[] CAMERA_OFF_COMMAND = {(byte)0xAA, 0x00, 0x08, 0x35, 0x01, 0x01}; // Location 2101 (0x0835), value 1 = Camera OFF
    private static final byte[] TRAIN_BACK_COMMAND = {(byte)0xAA, 0x00, 0x08, 0x46, 0x03, 0x02, 0x00, 0x01};    // Location 2118 (0x0846): skip direction=backward, 1 train
    private static final byte[] TRAIN_FORWARD_COMMAND = {(byte)0xAA, 0x00, 0x08, 0x46, 0x03, 0x01, 0x00, 0x01}; // Location 2118 (0x0846): skip direction=forward, 1 train

    // Control mode: null = not on control screen, "TREATMENT"/"MANUAL"/"MAPPING"/"CALIBRATE" = on control screen
    private volatile String controlMode = null;

    // Per-device flag: suppress the next auto-log-collection trigger (set by JS before discard stop command)
    private final Map<String, Boolean> deviceSuppressNextLogCollection = new ConcurrentHashMap<>();

    // Native gateway flags for legacy compatibility
    private static boolean isNativeGateway = false;
    private static boolean isNativeGatewayJsEventEmitOnSerialportData = false;

  // ref to
  // https://github.com/google/guava/blob/6405852bbf453b14d097b8ec3bcae494334b357d/android/guava/src/com/google/common/primitives/UnsignedBytes.java
  private static int unsignedByteToInt(byte value) {
      return value & 0xFF;
  }

  public RNSerialportModule(ReactApplicationContext reactContext) {
    super(reactContext);
    mReactContext = reactContext;
    fillDriverList();
    startQueueProcessor();
  }

  @Override
  public String getName() {
    return TAG;
  }

  private final String ACTION_USB_READY = "com.felhr.connectivityservices.USB_READY";
  private final String ACTION_USB_ATTACHED = "android.hardware.usb.action.USB_DEVICE_ATTACHED";
  private final String ACTION_USB_DETACHED = "android.hardware.usb.action.USB_DEVICE_DETACHED";
  private final String ACTION_USB_NOT_SUPPORTED = "com.felhr.usbservice.USB_NOT_SUPPORTED";
  private final String ACTION_NO_USB = "com.felhr.usbservice.NO_USB";
  private final String ACTION_USB_PERMISSION_GRANTED = "com.felhr.usbservice.USB_PERMISSION_GRANTED";
  private final String ACTION_USB_PERMISSION_NOT_GRANTED = "com.felhr.usbservice.USB_PERMISSION_NOT_GRANTED";
  private final String ACTION_USB_DISCONNECTED = "com.felhr.usbservice.USB_DISCONNECTED";
  private final String ACTION_USB_PERMISSION = "com.android.example.USB_PERMISSION";
  private final String ACTION_USB_NOT_OPENED = "com.melihyarikkaya.rnserialport.USB_NOT_OPENED";
  private final String ACTION_USB_CONNECT = "com.melihyarikkaya.rnserialport.USB_CONNECT";

  private final String EXTRA_USB_DEVICE_NAME = "com.melihyarikkaya.rnserialport.USB_DEVICE_NAME";

  //react-native events
  private final String onErrorEvent              = "onError";
  private final String onConnectedEvent          = "onConnected";
  private final String onDisconnectedEvent       = "onDisconnected";
  private final String onDeviceAttachedEvent     = "onDeviceAttached";
  private final String onDeviceDetachedEvent     = "onDeviceDetached";
  private final String onServiceStarted          = "onServiceStarted";
  private final String onServiceStopped          = "onServiceStopped";
  private final String onReadDataFromPort        = "onReadDataFromPort";
  private final String onUsbPermissionGranted    = "onUsbPermissionGranted";

  //SUPPORTED DRIVER LIST

  private List<String> driverList;

  private UsbManager usbManager;
  public Map<String, UsbSerialDevice> serialPorts = new HashMap<>(); // alias deviceName2SerialPort
    // Initialize auto-ramp engine with reference to serial ports
    private final AutoRampEngine autoRampEngine = new AutoRampEngine(serialPorts, crcEnabledByDevice);
  public Map<Integer, String> appBus2DeviceName = new HashMap<>(); // App define which bus id match which deviceName
  public Map<String, Integer> deviceName2SocketId = new HashMap<>();

  //Connection Settings
  private int DATA_BIT     = 9;
  private int STOP_BIT     = UsbSerialInterface.STOP_BITS_1;
  private int PARITY       = UsbSerialInterface.PARITY_EVEN;
  private int FLOW_CONTROL = UsbSerialInterface.FLOW_CONTROL_OFF;
  private int BAUD_RATE = 115200;


  private boolean autoConnect = false;
  private String autoConnectDeviceName;
  private int autoConnectBaudRate = 115200;
  private int portInterface = -1;
  private int returnedDataType = Definitions.RETURNED_DATA_TYPE_INTARRAY;
  private String driver = "AUTO";


  private boolean usbServiceStarted = false;
  private volatile boolean usbServiceReady = false;

  // Permission state tracking to prevent duplicate prompts
  private volatile boolean mPermissionRequested = false;
  private volatile boolean mHasPermission = false;
  private final Object mPermissionLock = new Object();

  // Debounce handling for USB attach events
  private Handler mAttachHandler = new Handler(Looper.getMainLooper());
  private Runnable mAttachRunnable = null;
  private static final int ATTACH_DEBOUNCE_MS = 300;

  private final BroadcastReceiver mUsbReceiver = new BroadcastReceiver() {
    @Override
    public void onReceive(Context arg0, Intent arg1) {
      Intent intent;
      String action = arg1.getAction();
      if (action == null) return;

      Bundle extras = arg1.getExtras();

      switch (arg1.getAction()) {
        case ACTION_USB_CONNECT:
          eventEmit(onConnectedEvent, extras.getString(EXTRA_USB_DEVICE_NAME));
          startHeartbeat(extras.getString(EXTRA_USB_DEVICE_NAME));
          break;
        case ACTION_USB_DISCONNECTED:
          String disconnectedDevice = extras.getString(EXTRA_USB_DEVICE_NAME);

          if (disconnectedDevice != null &&
              deviceLoggingActive.getOrDefault(disconnectedDevice, false)) {
              android.util.Log.w(TAG, "USB_DISCONNECT: Device was collecting logs, cleaning up");
              cleanupLogCollectionState(disconnectedDevice, true);
              emitLogError(disconnectedDevice, "Device disconnected during log collection");
          }

          if (disconnectedDevice != null) {
              temperatureCache.remove(disconnectedDevice);
          }

          eventEmit(onDisconnectedEvent, disconnectedDevice);

          synchronized(mPermissionLock) {
              mHasPermission = false;
              mPermissionRequested = false;
          }
          break;
        case ACTION_USB_NOT_SUPPORTED:
          eventEmit(onErrorEvent, createError(Definitions.ERROR_DEVICE_NOT_SUPPORTED, Definitions.ERROR_DEVICE_NOT_SUPPORTED_MESSAGE));
          break;
        case ACTION_USB_NOT_OPENED:
          eventEmit(onErrorEvent, createError(Definitions.ERROR_COULD_NOT_OPEN_SERIALPORT, Definitions.ERROR_COULD_NOT_OPEN_SERIALPORT_MESSAGE));
          break;
        case ACTION_USB_ATTACHED: {
          if (extras == null) break;
          UsbDevice device = extras.getParcelable(UsbManager.EXTRA_DEVICE);
          String deviceName = device.getDeviceName();

          boolean isTargetDevice = false;
          for (int pid : TARGET_PRODUCT_IDS) {
                  if (device.getProductId() == pid) {
                      isTargetDevice = true;
                      break;
                  }
          }
          if (!isTargetDevice) {
            break;
          }
          eventEmit(onDeviceAttachedEvent, deviceName);
          if(autoConnect && chooseFirstDevice()) {
            connectDevice(autoConnectDeviceName, autoConnectBaudRate);
          }
        }
          break;
        case ACTION_USB_DETACHED: {
          if (extras == null) break;
          UsbDevice device = extras.getParcelable(UsbManager.EXTRA_DEVICE);
          if (device == null) break;

          boolean isTargetDevice = false;
          for (int pid : TARGET_PRODUCT_IDS) {
                  if (device.getProductId() == pid) {
                      isTargetDevice = true;
                      break;
                  }
          }
          if (!isTargetDevice) {
            break;
          }

          String deviceName = device.getDeviceName();
          eventEmit(onDeviceDetachedEvent, deviceName);
          stopConnection(deviceName);
          serialPorts.remove(deviceName);
          if (deviceName != null) appBus2DeviceName.values().removeIf(deviceName::equals);
          synchronized(mPermissionLock) {
              mHasPermission = false;
              mPermissionRequested = false;
          }
        }
          break;
        case ACTION_USB_PERMISSION: {
          if (extras == null) break;
          UsbDevice device = extras.getParcelable(UsbManager.EXTRA_DEVICE);
          boolean granted = extras.getBoolean(UsbManager.EXTRA_PERMISSION_GRANTED);
          startConnection(device, granted);
        }
          break;
        case ACTION_USB_PERMISSION_GRANTED:
          eventEmit(onUsbPermissionGranted, null);
          break;
        case ACTION_USB_PERMISSION_NOT_GRANTED:
          android.util.Log.d(TAG, "USB permission denied by user");
          synchronized(mPermissionLock) {
            mPermissionRequested = false;  // Allow retry if user wants to reconnect
          }
          eventEmit(onErrorEvent, createError(Definitions.ERROR_USER_DID_NOT_ALLOW_TO_CONNECT, Definitions.ERROR_USER_DID_NOT_ALLOW_TO_CONNECT_MESSAGE));
          break;
      }
    }
  };

  private void eventEmit(String eventName, Object data) {
    try {
      if(mReactContext.hasActiveCatalystInstance()) {
        mReactContext.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class).emit(eventName, data);
      }
    }
    catch (Exception error) {}
  }

  private WritableMap createError(int code, String message) {
    WritableMap err = Arguments.createMap();
    err.putBoolean("status", false);
    err.putInt("errorCode", code);
    err.putString("errorMessage", message);

    return err;
  }

  private WritableMap createError(String deviceName, int code, String message) {
    WritableMap err = Arguments.createMap();
    err.putString("deviceName", deviceName);
    err.putBoolean("status", false);
    err.putInt("errorCode", code);
    err.putString("errorMessage", message);

    return err;
  }

  private void setFilters() {
    IntentFilter filter = new IntentFilter();
    filter.addAction(ACTION_USB_PERMISSION_GRANTED);
    filter.addAction(ACTION_NO_USB);
    filter.addAction(ACTION_USB_CONNECT);
    filter.addAction(ACTION_USB_DISCONNECTED);
    filter.addAction(ACTION_USB_NOT_SUPPORTED);
    filter.addAction(ACTION_USB_PERMISSION_NOT_GRANTED);
    filter.addAction(ACTION_USB_PERMISSION);
    filter.addAction(ACTION_USB_ATTACHED);
    filter.addAction(ACTION_USB_DETACHED);
    
     if (android.os.Build.VERSION.SDK_INT >= 34) {
        mReactContext.registerReceiver(mUsbReceiver, filter, Context.RECEIVER_EXPORTED);
    } else {
        mReactContext.registerReceiver(mUsbReceiver, filter);
    }
  }

  private void fillDriverList() {
    driverList = new ArrayList<>();
    driverList.add("ftdi");
    driverList.add("cp210x");
    driverList.add("pl2303");
    driverList.add("ch34x");
    driverList.add("cdc");
  }

  /******************************* BEGIN PUBLIC SETTER METHODS **********************************/

  @ReactMethod
  public void setDataBit(int DATA_BIT) {
    this.DATA_BIT = DATA_BIT;
  }
  @ReactMethod
  public void setStopBit(int STOP_BIT) {
    this.STOP_BIT = STOP_BIT;
  }
  @ReactMethod
  public void setParity(int PARITY) {
    this.PARITY = PARITY;
  }
  @ReactMethod
  public void setFlowControl(int FLOW_CONTROL) {
    this.FLOW_CONTROL = FLOW_CONTROL;
  }

  @ReactMethod
  public void loadDefaultConnectionSetting() {
    DATA_BIT     = UsbSerialInterface.DATA_BITS_8;
    STOP_BIT     = UsbSerialInterface.STOP_BITS_1;
    PARITY       =  UsbSerialInterface.PARITY_NONE;
    FLOW_CONTROL = UsbSerialInterface.FLOW_CONTROL_OFF;
  }
  @ReactMethod
  public void setAutoConnect(boolean autoConnect) {
    this.autoConnect = autoConnect;
  }
  @ReactMethod
  public void setAutoConnectBaudRate(int baudRate) {
    this.autoConnectBaudRate = baudRate;
  }
  @ReactMethod
  public void setInterface(int iFace) {
    this.portInterface = iFace;
  }
  @ReactMethod
  public void setReturnedDataType(int type) {
    if(type == Definitions.RETURNED_DATA_TYPE_HEXSTRING || type == Definitions.RETURNED_DATA_TYPE_INTARRAY) {
      this.returnedDataType = type;
    }
  }

  @ReactMethod
  public void setControlMode(String mode) {
    android.util.Log.d(TAG, "setControlMode: " + mode);

    String previousMode = this.controlMode;
    this.controlMode = mode;

    // Clear timestamp validator on control mode transitions (not reconnections)
    timestampValidator.clear();

    // Leaving control screen -> turn camera OFF
    if (mode == null && previousMode != null && heartbeatDevice != null) {
      if (serialPorts.containsKey(heartbeatDevice)) {
        addToNativeQueue(heartbeatDevice, CAMERA_OFF_COMMAND, PRIORITY_NORMAL, "camera_off_leave_control", 0);
      }
      deviceSuppressNextLogCollection.clear();
    }

    // Entering control screen -> turn camera ON
    if (mode != null && previousMode == null && heartbeatDevice != null) {
      if (serialPorts.containsKey(heartbeatDevice)) {
        addToNativeQueue(heartbeatDevice, CAMERA_ON_COMMAND, PRIORITY_NORMAL, "camera_on_enter", 0);
      }
    }
  }

  @ReactMethod
  public void setDriver(String driver) {
    if(driver.isEmpty() || !driverList.contains(driver.trim())) {
      eventEmit(onErrorEvent, createError(Definitions.ERROR_DRIVER_TYPE_NOT_FOUND, Definitions.ERROR_DRIVER_TYPE_NOT_FOUND_MESSAGE));
      return;
    }

    this.driver = driver;
  }

  /********************************************* END **********************************************/

  @ReactMethod
  public void startUsbService() {
    if(usbServiceStarted) {
      return;
    }
    setFilters();

    usbManager = (UsbManager) mReactContext.getSystemService(Context.USB_SERVICE);

    usbServiceStarted = true;

    //Return usb status when service is started.
    WritableMap map = Arguments.createMap();

    map.putBoolean("deviceAttached", !usbManager.getDeviceList().isEmpty());

    eventEmit(onServiceStarted, map);

    // Mark service as ready after initialization is complete
    usbServiceReady = true;
    
    checkAutoConnect();
  }

  @ReactMethod
  public void stopUsbService() {
    if(!serialPorts.isEmpty()) {
      eventEmit(onErrorEvent, createError(Definitions.ERROR_SERVICE_STOP_FAILED, Definitions.ERROR_SERVICE_STOP_FAILED_MESSAGE));
      return;
    }
    if(!usbServiceStarted) {
      return;
    }
    mReactContext.unregisterReceiver(mUsbReceiver);
    usbServiceStarted = false;
    usbServiceReady = false;
    eventEmit(onServiceStopped, null);
  }

  @Override
  public void onHostResume() {}

  @Override
  public void onHostPause() {}

  @Override
  public void onHostDestroy() {}

  @Override
  public void invalidate() {
    super.invalidate();
    disconnectAllDevices();
    stopUsbService();
  }

  @ReactMethod
  public void getDeviceList(Promise promise) {
    if(!usbServiceStarted) {
      promise.reject(String.valueOf(Definitions.ERROR_USB_SERVICE_NOT_STARTED), Definitions.ERROR_USB_SERVICE_NOT_STARTED_MESSAGE);
      return;
    }

    UsbManager manager = (UsbManager) mReactContext.getSystemService(Context.USB_SERVICE);

    HashMap<String, UsbDevice> devices = manager.getDeviceList();

    if(devices.isEmpty()) {
      //promise.reject(String.valueOf(Definitions.ERROR_DEVICE_NOT_FOUND), Definitions.ERROR_DEVICE_NOT_FOUND_MESSAGE);
      promise.resolve(Arguments.createArray());
      return;
    }

    WritableArray deviceList = Arguments.createArray();
    for(Map.Entry<String, UsbDevice> entry: devices.entrySet()) {
      UsbDevice d = entry.getValue();

      WritableMap map = Arguments.createMap();
      map.putString("name", d.getDeviceName());
      map.putInt("vendorId", d.getVendorId());
      map.putInt("productId", d.getProductId());
      map.putInt("class", d.getDeviceClass());
      map.putInt("subclass", d.getDeviceSubclass());

      deviceList.pushMap(map);
    }

    promise.resolve(deviceList);
  }

  @ReactMethod
  public void connectDevice(String deviceName, int baudRate) {
    try {
      if(!usbServiceStarted){
        eventEmit(onErrorEvent, createError(Definitions.ERROR_USB_SERVICE_NOT_STARTED, Definitions.ERROR_USB_SERVICE_NOT_STARTED_MESSAGE));
        return;
      }
      
      // Wait for USB service to be fully ready before connection attempt
      // This prevents race condition with React Native 0.80 threading model
      if (!isUsbServiceReady()) {
        // Brief wait and retry to ensure service is fully initialized
        executorService.execute(new Runnable() {
          @Override
          public void run() {
            int retryCount = 0;
            int maxRetries = 10; // Max 1 second wait (100ms * 10)
            while (!isUsbServiceReady() && retryCount < maxRetries) {
              try {
                Thread.sleep(100); // 100ms wait between retries
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
              }
              retryCount++;
            }
            // Now attempt connection with fully initialized service
            connectDeviceInternal(deviceName, baudRate);
          }
        });
        return;
      }

      // Service is ready, proceed with direct connection
      connectDeviceInternal(deviceName, baudRate);
    } catch (Exception err) {
      eventEmit(onErrorEvent, createError(deviceName, Definitions.ERROR_CONNECTION_FAILED, Definitions.ERROR_CONNECTION_FAILED_MESSAGE + " Catch Error Message:" + err.getMessage()));
    }
  }

  /**
   * Internal connection method with full validation logic
   * Separated to support both direct and retry-based connection attempts
   */
  private void connectDeviceInternal(String deviceName, int baudRate) {
    try {
      if(deviceName == null || deviceName.isEmpty()) {
        eventEmit(onErrorEvent, createError(Definitions.ERROR_CONNECT_DEVICE_NAME_INVALID, Definitions.ERROR_CONNECT_DEVICE_NAME_INVALID_MESSAGE));
        return;
      }

      if(serialPorts.get(deviceName) != null) {
        eventEmit(onErrorEvent, createError(deviceName, Definitions.ERROR_SERIALPORT_ALREADY_CONNECTED, Definitions.ERROR_SERIALPORT_ALREADY_CONNECTED_MESSAGE));
        return;
      }

      if(baudRate < 1){
        eventEmit(onErrorEvent, createError(deviceName, Definitions.ERROR_CONNECT_BAUDRATE_EMPTY, Definitions.ERROR_CONNECT_BAUDRATE_EMPTY_MESSAGE));
        return;
      }

      if(!autoConnect) {
        this.BAUD_RATE = 115200;
      }

      UsbDevice device = chooseDevice(deviceName);

      if(device == null) {
        eventEmit(onErrorEvent, createError(deviceName, Definitions.ERROR_X_DEVICE_NOT_FOUND, Definitions.ERROR_X_DEVICE_NOT_FOUND_MESSAGE + deviceName));
        return;
      }

      if (usbManager.hasPermission(device)) {
        startConnection(device, true);
      } else {
        requestUserPermission(device);
      }

    } catch (Exception err) {
      eventEmit(onErrorEvent, createError(deviceName, Definitions.ERROR_CONNECTION_FAILED, Definitions.ERROR_CONNECTION_FAILED_MESSAGE + " Catch Error Message:" + err.getMessage()));
    }
  }

  @ReactMethod
  public void disconnectDevice(String deviceName) {
    if(!usbServiceStarted){
      eventEmit(onErrorEvent, createError(Definitions.ERROR_USB_SERVICE_NOT_STARTED, Definitions.ERROR_USB_SERVICE_NOT_STARTED_MESSAGE));
      return;
    }

    if(!serialPorts.containsKey(deviceName)) {
      eventEmit(onErrorEvent, createError(Definitions.ERROR_SERIALPORT_ALREADY_DISCONNECTED, Definitions.ERROR_SERIALPORT_ALREADY_DISCONNECTED_MESSAGE));
      return;
    }
    stopConnection(deviceName);
    serialPorts.remove(deviceName);
  }

  @ReactMethod
  public void disconnectAllDevices() {
    if(!usbServiceStarted){
      eventEmit(onErrorEvent, createError(Definitions.ERROR_USB_SERVICE_NOT_STARTED, Definitions.ERROR_USB_SERVICE_NOT_STARTED_MESSAGE));
      return;
    }

    // Intent intent = new Intent(ACTION_USB_DETACHED);
    // mReactContext.sendBroadcast(intent);

    // above will cause
    // `Permission Denial: not allowed to send broadcast android.hardware.usb.action.USB_DEVICE_DETACHED from pid=6037, uid=10068`
    // so use below instead

    for(Map.Entry<String, UsbSerialDevice> entry: serialPorts.entrySet()) {
      stopConnection(entry.getKey());
    }
    serialPorts.clear();
    appBus2DeviceName.clear();
  }

  @ReactMethod
  public void isOpen(String deviceName, Promise promise) {
    promise.resolve(serialPorts.containsKey(deviceName));
  }

 @ReactMethod
 public void isServiceStarted(Promise promise) {
    promise.resolve(usbServiceStarted);
 }
 
 /**
  * Check if USB service is fully ready for connections
  * This prevents race conditions in React Native 0.80 threading model
  */
 private boolean isUsbServiceReady() {
    return usbServiceStarted && usbServiceReady && usbManager != null;
 }

  @ReactMethod
  public void isSupported(String deviceName, Promise promise) {
    UsbDevice device = chooseDevice(deviceName);

    if(device == null) {
      promise.reject(String.valueOf(Definitions.ERROR_DEVICE_NOT_FOUND), Definitions.ERROR_DEVICE_NOT_FOUND_MESSAGE);
    } else {
      promise.resolve(UsbSerialDevice.isSupported(device));
    }
  }

  public void writeSerialportBytes(String deviceName, byte[] bytes) {
    if(!usbServiceStarted){
      eventEmit(onErrorEvent, createError(Definitions.ERROR_USB_SERVICE_NOT_STARTED, Definitions.ERROR_USB_SERVICE_NOT_STARTED_MESSAGE));
      return;
    }
    UsbSerialDevice serialPort = serialPorts.get(deviceName);
    if(serialPort == null) {
      eventEmit(onErrorEvent, createError(Definitions.ERROR_THERE_IS_NO_CONNECTION, Definitions.ERROR_THERE_IS_NO_CONNECTION_MESSAGE));
      return;
    }

    // Drop any orphan bytes from the previous response window before issuing a new request.
    // In normal operation the buffer is empty here (response was extracted ~30ms after prev TX,
    // 220ms before this TX). Non-empty means a truncated/aborted prior response left orphans;
    // appending the new response to those orphans causes cross-frame extraction (cascade bug).
    NativePacketBuffer staleBuffer = devicePacketBuffers.get(deviceName);
    if (staleBuffer != null) {
      staleBuffer.clear();
    }

    // Append the 16-bit CRC to every outgoing command — both read (0x66) and write (0xAA)
    // requests carry a CRC once the PG firmware supports it (>= V0.16). A legacy PG never
    // enables CRC, so this is a no-op for older firmware. (The write ACK that comes back
    // from the PG carries NO CRC; that asymmetry is handled on the RX side.)
    byte[] outgoing = bytes;
    if (bytes.length > 0 && (bytes[0] == (byte) 0x66 || bytes[0] == (byte) 0xAA)
        && isCrcEnabled(deviceName)) {
      outgoing = Crc16.append(bytes);
    }

    // Log the write operation
    android.util.Log.i(TAG, String.format("[TX] writeSerialportBytes: device=%s, bytes=%d, data=%s",
        deviceName, outgoing.length, Definitions.bytesToHex(outgoing)));

    serialPort.write(outgoing);
  }

  @ReactMethod
  public void writeBytes(String deviceName, ReadableArray message) {
    if(!usbServiceStarted){
      eventEmit(onErrorEvent, createError(Definitions.ERROR_USB_SERVICE_NOT_STARTED, Definitions.ERROR_USB_SERVICE_NOT_STARTED_MESSAGE));
      return;
    }
    UsbSerialDevice serialPort = serialPorts.get(deviceName);
    if(serialPort == null) {
      eventEmit(onErrorEvent, createError(Definitions.ERROR_THERE_IS_NO_CONNECTION, Definitions.ERROR_THERE_IS_NO_CONNECTION_MESSAGE));
      return;
    }
    int length = message.size();
    byte [] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte)message.getInt(i);
    }

    // Log the write operation
    android.util.Log.i(TAG, String.format("[TX] writeBytes: device=%s, bytes=%d, data=%s",
        deviceName, bytes.length, Definitions.bytesToHex(bytes)));

    serialPort.write(bytes);
  }

  @ReactMethod
  public void writeString(String deviceName, String message) {
    if(!usbServiceStarted){
      eventEmit(onErrorEvent, createError(Definitions.ERROR_USB_SERVICE_NOT_STARTED, Definitions.ERROR_USB_SERVICE_NOT_STARTED_MESSAGE));
      return;
    }
    UsbSerialDevice serialPort = serialPorts.get(deviceName);
    if(serialPort == null) {
      eventEmit(onErrorEvent, createError(Definitions.ERROR_THERE_IS_NO_CONNECTION, Definitions.ERROR_THERE_IS_NO_CONNECTION_MESSAGE));
      return;
    }

    byte[] bytes = message.getBytes();

    // Log the write operation
    android.util.Log.i(TAG, String.format("[TX] writeString: device=%s, bytes=%d, string=\"%s\", data=%s",
        deviceName, bytes.length, message, Definitions.bytesToHex(bytes)));

    serialPort.write(bytes);
  }

  @ReactMethod
  public void writeBase64(String deviceName, String message) {
    if(!usbServiceStarted){
      eventEmit(onErrorEvent, createError(Definitions.ERROR_USB_SERVICE_NOT_STARTED, Definitions.ERROR_USB_SERVICE_NOT_STARTED_MESSAGE));
      return;
    }
    UsbSerialDevice serialPort = serialPorts.get(deviceName);
    if(serialPort == null) {
      eventEmit(onErrorEvent, createError(Definitions.ERROR_THERE_IS_NO_CONNECTION, Definitions.ERROR_THERE_IS_NO_CONNECTION_MESSAGE));
      return;
    }

    byte [] data = Base64.decode(message, Base64.DEFAULT);

    // Log the write operation
    android.util.Log.i(TAG, String.format("[TX] writeBase64: device=%s, bytes=%d, base64=\"%s\", data=%s",
        deviceName, data.length, message, Definitions.bytesToHex(data)));

    serialPort.write(data);
  }

  @ReactMethod
  public void writeHexString(String deviceName, String message) {
    if(!usbServiceStarted){
      eventEmit(onErrorEvent, createError(Definitions.ERROR_USB_SERVICE_NOT_STARTED, Definitions.ERROR_USB_SERVICE_NOT_STARTED_MESSAGE));
      return;
    }
    UsbSerialDevice serialPort = serialPorts.get(deviceName);
    if(serialPort == null) {
      eventEmit(onErrorEvent, createError(Definitions.ERROR_THERE_IS_NO_CONNECTION, Definitions.ERROR_THERE_IS_NO_CONNECTION_MESSAGE));
      return;
    }

    String msg = message.toUpperCase();

    if(msg.length() < 1) {
      return;
    }

    byte[] data = new byte[msg.length() / 2];
    for (int i = 0; i < data.length; i++) {
      int index = i * 2;

      String hex = msg.substring(index, index + 2);

      if(Definitions.hexChars.indexOf(hex.substring(0, 1)) == -1 || Definitions.hexChars.indexOf(hex.substring(1, 1)) == -1) {
          return;
      }

      int v = Integer.parseInt(hex, 16);
      data[i] = (byte) v;
    }

    // Log the write operation
    android.util.Log.i(TAG, String.format("[TX] writeHexString: device=%s, bytes=%d, hex=\"%s\", data=%s",
        deviceName, data.length, message, Definitions.bytesToHex(data)));

    serialPort.write(data);
  }

  ///////////////////////////////////////////////USB SERVICE /////////////////////////////////////////////////////////
  ///////////////////////////////////////////////USB SERVICE /////////////////////////////////////////////////////////

  private UsbDevice chooseDevice(String deviceName) {
    HashMap<String, UsbDevice> usbDevices = usbManager.getDeviceList();
    if(usbDevices.isEmpty()) {
      return null;
    }

    UsbDevice device = null;

    for (Map.Entry<String, UsbDevice> entry: usbDevices.entrySet()) {
      UsbDevice d = entry.getValue();

      if(d.getDeviceName().equals(deviceName)) {
        device = d;
        break;
      }
    }

    return device;
  }

  private boolean chooseFirstDevice() {
    HashMap<String, UsbDevice> usbDevices = usbManager.getDeviceList();
    if(usbDevices.isEmpty()) {
      return false;
    }

    boolean selected = false;

    for (Map.Entry<String, UsbDevice> entry: usbDevices.entrySet()) {
      UsbDevice d = entry.getValue();

      int deviceVID = d.getVendorId();
      int devicePID = d.getProductId();

      boolean isTargetDevice = false;
        for (int pid : TARGET_PRODUCT_IDS) {
            if (d.getProductId() == pid) {
                isTargetDevice = true;
                break;
            }
      }

      if (!isTargetDevice) {
        continue;
      }

      if (isTargetDevice) {
        autoConnectDeviceName = d.getDeviceName();
        selected = true;
        break;
      }

      if (deviceVID != 0x1d6b && (devicePID != 0x0001 && devicePID != 0x0002 && devicePID != 0x0003) && deviceVID != 0x5c6 && devicePID != 0x904c)
      {
        autoConnectDeviceName = d.getDeviceName();
        selected = true;
        break;
      }
    }
    return selected;
  }

  private void checkAutoConnect() {
    if(!autoConnect || !serialPorts.isEmpty())
      return;

    if(chooseFirstDevice()) {
      connectDevice(autoConnectDeviceName, autoConnectBaudRate);
    }
  }
  private class ConnectionThread extends Thread {
    private UsbDevice device;
    private UsbDeviceConnection connection;

    public ConnectionThread(UsbDevice device, UsbDeviceConnection connection) {
        this.device = device;
        this.connection = connection;
    }

    @Override
    public void run() {
      try {
        UsbSerialDevice serialPort;
        if(driver.equals("AUTO")) {
          serialPort = UsbSerialDevice.createUsbSerialDevice(device, connection, portInterface);
        } else {
          serialPort = UsbSerialDevice.createUsbSerialDevice(driver, device, connection, portInterface);
        }
        if(serialPort == null) {
          // No driver for given device
          Intent intent = new Intent(ACTION_USB_NOT_SUPPORTED);
          mReactContext.sendBroadcast(intent);
          return;
        }

        if(!serialPort.open()){
          Intent intent = new Intent(ACTION_USB_NOT_OPENED);
          mReactContext.sendBroadcast(intent);
          return;
        }

        serialPorts.put(device.getDeviceName(), serialPort);
        int baud;
        if(autoConnect){
          baud = autoConnectBaudRate;
        }else {
          baud = BAUD_RATE;
        }
        serialPort.setBaudRate(baud);
        serialPort.setDataBits(DATA_BIT);
        serialPort.setStopBits(STOP_BIT);
        serialPort.setParity(PARITY);
        serialPort.setFlowControl(FLOW_CONTROL);

        UsbSerialInterface.UsbReadCallback usbReadCallback = new UsbSerialInterface.UsbReadCallback() {
          @Override
          public void onReceivedData(byte[] bytes) {
            if (bytes.length == 0) {
              return;
            }

            android.util.Log.i(TAG, "[RX] " + bytes.length + " bytes: " + Definitions.bytesToHex(bytes));

            final byte[] bytesCopy = Arrays.copyOf(bytes, bytes.length);
            final String deviceName = device.getDeviceName();

            if (usbProcessingExecutor == null || usbProcessingExecutor.isShutdown() || usbProcessingExecutor.isTerminated()) {
              return;
            }

            usbProcessingExecutor.execute(new Runnable() {
              @Override
              public void run() {
                processNativePackets(deviceName, bytesCopy);
              }
            });
          }
        };
        serialPort.read(usbReadCallback);

        Intent intent = new Intent(ACTION_USB_READY);
        mReactContext.sendBroadcast(intent);
        intent = new Intent(ACTION_USB_CONNECT);
        intent.putExtra(EXTRA_USB_DEVICE_NAME, device.getDeviceName());
        mReactContext.sendBroadcast(intent);
      } catch (Exception error) {
        WritableMap map = createError(Definitions.ERROR_CONNECTION_FAILED, Definitions.ERROR_CONNECTION_FAILED_MESSAGE);
        map.putString("exceptionErrorMessage", error.getMessage());
        eventEmit(onErrorEvent, map);
      }
    }
  }

  private void requestUserPermission(UsbDevice device) {
    if(device == null)
      return;
    Intent permissionIntent = new Intent(ACTION_USB_PERMISSION);
    permissionIntent.putExtra(UsbManager.EXTRA_DEVICE, device);
    permissionIntent.setPackage(mReactContext.getPackageName());
    PendingIntent mPendingIntent = PendingIntent.getBroadcast(mReactContext, 0 , permissionIntent, PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_MUTABLE);
    usbManager.requestPermission(device, mPendingIntent);
  }

  private void startConnection(UsbDevice device, boolean granted) {
    if(granted) {
      Intent intent = new Intent(ACTION_USB_PERMISSION_GRANTED);
      mReactContext.sendBroadcast(intent);
      UsbDeviceConnection connection = usbManager.openDevice(device);
      new ConnectionThread(device, connection).start();
    } else {
      Intent intent = new Intent(ACTION_USB_PERMISSION_NOT_GRANTED);
      mReactContext.sendBroadcast(intent);
    }
  }

  /**
   * FIX TMS-APP-37: Stop USB connection with proper synchronization
   * Prevents libusb_handle_events_timeout_completed crash by:
   * 1. Removing serial port from map FIRST (prevents new events)
   * 2. Allowing brief time for pending events to complete
   * 3. Then closing the connection safely with exception handling
   */
  private void stopConnection(String deviceName) {
    android.util.Log.i(TAG, "🛑 stopConnection: Starting graceful shutdown for: " + deviceName);

    UsbSerialDevice serialPort = serialPorts.get(deviceName);
    if(serialPort == null) {
      android.util.Log.w(TAG, "🛑 stopConnection: No active connection for: " + deviceName);
      eventEmit(onErrorEvent, createError(Definitions.ERROR_THERE_IS_NO_CONNECTION, Definitions.ERROR_THERE_IS_NO_CONNECTION_MESSAGE));
      return;
    }

    // FIX TMS-APP-37: Remove from map FIRST to prevent new event emissions
    // This ensures libusb won't try to emit events after we start closing
    serialPorts.remove(deviceName);
    android.util.Log.d(TAG, "🛑 stopConnection: Removed serial port from active map");

    // Remove the per-device packet buffer entirely: the device is disconnecting, so its
    // buffer reaches true end-of-life here and is reclaimed. A future reconnect (even with the
    // same deviceName) mints a fresh buffer via computeIfAbsent, so no stale data can desync it.
    // (Log-collection completion only clear()s the buffer, because the device stays connected
    // there and its CRC framing must be preserved.)
    NativePacketBuffer removedBuffer = devicePacketBuffers.remove(deviceName);
    if (removedBuffer != null) {
      android.util.Log.i(TAG, "🛑 stopConnection: Removed packet buffer for: " + deviceName);
    }

    // Forget CRC enablement + cached firmware identity so a reconnected (or different) PG
    // re-evaluates its firmware version before CRC is applied again.
    crcEnabledByDevice.remove(deviceName);
    packetIntegrityValidator.reset();

    // Give libusb time to finish processing any pending events
    // This prevents SIGSEGV in libusb_handle_events_timeout_completed
    try {
      Thread.sleep(100); // 100ms is enough for libusb event queue to drain
    } catch (InterruptedException e) {
      android.util.Log.w(TAG, "🛑 stopConnection: Sleep interrupted: " + e.getMessage());
      Thread.currentThread().interrupt();
    }

    // Now safely close the connection with exception handling
    try {
      serialPort.close();
      android.util.Log.i(TAG, "✅ stopConnection: USB connection closed successfully for: " + deviceName);
    } catch (Exception e) {
      // Catch any exceptions from close() to prevent crash propagation
      android.util.Log.e(TAG, "❌ stopConnection: Error closing USB connection (non-fatal): " + e.getMessage(), e);
    }

    if (deviceName != null) appBus2DeviceName.values().removeIf(deviceName::equals);

    Intent intent = new Intent(ACTION_USB_DISCONNECTED);
    intent.putExtra(EXTRA_USB_DEVICE_NAME, deviceName);
    mReactContext.sendBroadcast(intent);
  }

  ///////////////////////////////////////////////TCP Socket /////////////////////////////////////////////////////////
  ///////////////////////////////////////////////TCP Socket /////////////////////////////////////////////////////////

    private void sendEvent(String eventName, WritableMap params) {
        mReactContext
                .getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class)
                .emit(eventName, params);
    }

    /**
     * Creates a TCP Socket and establish a connection with the given host
     *
     * @param cId     socket ID
     * @param host    socket IP address
     * @param port    socket port to be bound
     * @param options extra options
     */
    @SuppressLint("StaticFieldLeak")
    @SuppressWarnings("unused")
    @ReactMethod
    public void connect(@NonNull final Integer cId, @NonNull final String host, @NonNull final Integer port, @NonNull final ReadableMap options) {
        executorService.execute(new Thread(new Runnable() {
            @Override
            public void run() {
                if (socketMap.get(cId) != null) {
                    onError(cId, TAG + "createSocket called twice with the same id.");
                    return;
                }
                try {
                    // Get the network interface
                    final String localAddress = options.hasKey("localAddress") ? options.getString("localAddress") : null;
                    final String iface = options.hasKey("interface") ? options.getString("interface") : null;
                    selectNetwork(iface, localAddress);
                    TcpSocketClient client = new TcpSocketClient(RNSerialportModule.this, cId, null);
                    socketMap.put(cId, client);
                    client.connect(mReactContext, host, port, options, currentNetwork.getNetwork());
                    onConnect(cId, client);
                } catch (Exception e) {
                    onError(cId, e.getMessage());
                }
            }
        }));
    }

    public void writeSocketBytes(@NonNull final Integer cId, @NonNull final byte[] bytes) {
        TcpSocketClient socketClient = getTcpClient(cId);
        try {
            socketClient.write(bytes);
        } catch (IOException e) {
            onError(cId, e.toString());
        }
    }

    @SuppressLint("StaticFieldLeak")
    @SuppressWarnings("unused")
    @ReactMethod
    public void write(@NonNull final Integer cId, @NonNull final String base64String, @Nullable final Callback callback) {
        executorService.execute(new Thread(new Runnable() {
            @Override
            public void run() {
                TcpSocketClient socketClient = getTcpClient(cId);
                try {
                    socketClient.write(Base64.decode(base64String, Base64.NO_WRAP));
                    if (callback != null) {
                        callback.invoke();
                    }
                } catch (IOException e) {
                    if (callback != null) {
                        callback.invoke(e.toString());
                    }
                    onError(cId, e.toString());
                }
            }
        }));
    }

    @SuppressLint("StaticFieldLeak")
    @SuppressWarnings("unused")
    @ReactMethod
    public void end(final Integer cId) {
        executorService.execute(new Thread(new Runnable() {
            @Override
            public void run() {
                TcpSocketClient socketClient = getTcpClient(cId);
                socketClient.destroy();
                socketMap.remove(cId);
                deviceName2SocketId.values().removeIf(cId::equals);
            }
        }));
    }

    @SuppressWarnings("unused")
    @ReactMethod
    public void destroy(final Integer cId) {
        end(cId);
    }

    @SuppressWarnings("unused")
    @ReactMethod
    public void close(final Integer cId) {
        executorService.execute(new Thread(new Runnable() {
            @Override
            public void run() {
                TcpSocketServer socketServer = getTcpServer(cId);
                socketServer.close();
                socketMap.remove(cId);
                deviceName2SocketId.values().removeIf(cId::equals);
            }
        }));
    }

    @SuppressLint("StaticFieldLeak")
    @SuppressWarnings("unused")
    @ReactMethod
    public void listen(final Integer cId, final ReadableMap options) {
        executorService.execute(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    TcpSocketServer server = new TcpSocketServer(socketMap, RNSerialportModule.this, cId, options);
                    socketMap.put(cId, server);
                    onListen(cId, server);
                } catch (Exception uhe) {
                    onError(cId, uhe.getMessage());
                }
            }
        }));
    }

    @SuppressWarnings("unused")
    @ReactMethod
    public void setNoDelay(@NonNull final Integer cId, final boolean noDelay) {
        final TcpSocketClient client = getTcpClient(cId);
        try {
            client.setNoDelay(noDelay);
        } catch (IOException e) {
            onError(cId, e.getMessage());
        }
    }

    @SuppressWarnings("unused")
    @ReactMethod
    public void setKeepAlive(@NonNull final Integer cId, final boolean enable, final int initialDelay) {
        final TcpSocketClient client = getTcpClient(cId);
        try {
            client.setKeepAlive(enable, initialDelay);
        } catch (IOException e) {
            onError(cId, e.getMessage());
        }
    }

    private void requestNetwork(final int transportType) throws InterruptedException {
        final NetworkRequest.Builder requestBuilder = new NetworkRequest.Builder();
        requestBuilder.addTransportType(transportType);
        final CountDownLatch awaitingNetwork = new CountDownLatch(1); // only needs to be counted down once to release waiting threads
        final ConnectivityManager cm = (ConnectivityManager) mReactContext.getSystemService(Context.CONNECTIVITY_SERVICE);
        cm.requestNetwork(requestBuilder.build(), new ConnectivityManager.NetworkCallback() {
            @Override
            public void onAvailable(Network network) {
                currentNetwork.setNetwork(network);
                awaitingNetwork.countDown(); // Stop waiting
            }

            @Override
            public void onUnavailable() {
                awaitingNetwork.countDown(); // Stop waiting
            }
        });
        // Timeout if there the network is unreachable
        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);
        exec.schedule(new Runnable() {
            public void run() {
                awaitingNetwork.countDown(); // Stop waiting
            }
        }, 5, TimeUnit.SECONDS);
        awaitingNetwork.await();
    }

    // REQUEST NETWORK

    /**
     * Returns a network given its interface name:
     * "wifi" -> WIFI
     * "cellular" -> Cellular
     * etc...
     */
    private void selectNetwork(@Nullable final String iface, @Nullable final String ipAddress) throws InterruptedException, IOException {
        currentNetwork.setNetwork(null);
        if (iface == null) return;
        if (ipAddress != null) {
            final Network cachedNetwork = mNetworkMap.get(iface + ipAddress);
            if (cachedNetwork != null) {
                currentNetwork.setNetwork(cachedNetwork);
                return;
            }
        }
        switch (iface) {
            case "wifi":
                requestNetwork(NetworkCapabilities.TRANSPORT_WIFI);
                break;
            case "cellular":
                requestNetwork(NetworkCapabilities.TRANSPORT_CELLULAR);
                break;
            case "ethernet":
                requestNetwork(NetworkCapabilities.TRANSPORT_ETHERNET);
                break;
        }
        if (currentNetwork.getNetwork() == null) {
            throw new IOException("Interface " + iface + " unreachable");
        } else if (ipAddress != null && !ipAddress.equals("0.0.0.0"))
            mNetworkMap.put(iface + ipAddress, currentNetwork.getNetwork());
    }

    // TcpReceiverTask.OnDataReceivedListener

    @Override
    public void onConnect(Integer id, TcpSocketClient client) {
        WritableMap eventParams = Arguments.createMap();
        eventParams.putInt("id", id);
        WritableMap connectionParams = Arguments.createMap();
        final Socket socket = client.getSocket();
        final InetSocketAddress remoteAddress = (InetSocketAddress) socket.getRemoteSocketAddress();

        connectionParams.putString("localAddress", socket.getLocalAddress().getHostAddress());
        connectionParams.putInt("localPort", socket.getLocalPort());
        connectionParams.putString("remoteAddress", remoteAddress.getAddress().getHostAddress());
        connectionParams.putInt("remotePort", socket.getPort());
        connectionParams.putString("remoteFamily", remoteAddress.getAddress() instanceof Inet6Address ? "IPv6" : "IPv4");
        eventParams.putMap("connection", connectionParams);
        sendEvent("connect", eventParams);
    }

    @Override
    public void onListen(Integer id, TcpSocketServer server) {
        WritableMap eventParams = Arguments.createMap();
        eventParams.putInt("id", id);
        WritableMap connectionParams = Arguments.createMap();
        final ServerSocket serverSocket = server.getServerSocket();
        final InetAddress address = serverSocket.getInetAddress();

        connectionParams.putString("localAddress", serverSocket.getInetAddress().getHostAddress());
        connectionParams.putInt("localPort", serverSocket.getLocalPort());
        connectionParams.putString("localFamily", address instanceof Inet6Address ? "IPv6" : "IPv4");
        eventParams.putMap("connection", connectionParams);
        sendEvent("listening", eventParams);
    }

    @Override
    public void onData(Integer id, byte[] data) {
        // Always use native Gateway for TCP data processing
        Gateway.onSocketData(id, data, RNSerialportModule.this);
    }

    @Override
    public void onClose(Integer id, String error) {
        socketMap.remove(id);
        deviceName2SocketId.values().removeIf(id::equals);

        if (error != null) {
            onError(id, error);
        }
        WritableMap eventParams = Arguments.createMap();
        eventParams.putInt("id", id);
        eventParams.putBoolean("hadError", error != null);

        sendEvent("close", eventParams);
    }

    @Override
    public void onError(Integer id, String error) {
        WritableMap eventParams = Arguments.createMap();
        eventParams.putInt("id", id);
        eventParams.putString("error", error);

        sendEvent("error", eventParams);
    }

    @Override
    public void onConnection(Integer serverId, Integer clientId, Socket socket) {
        WritableMap eventParams = Arguments.createMap();
        eventParams.putInt("id", serverId);

        WritableMap infoParams = Arguments.createMap();
        infoParams.putInt("id", clientId);

        WritableMap connectionParams = Arguments.createMap();
        final InetSocketAddress remoteAddress = (InetSocketAddress) socket.getRemoteSocketAddress();

        connectionParams.putString("localAddress", socket.getLocalAddress().getHostAddress());
        connectionParams.putInt("localPort", socket.getLocalPort());
        connectionParams.putString("remoteAddress", remoteAddress.getAddress().getHostAddress());
        connectionParams.putInt("remotePort", socket.getPort());
        connectionParams.putString("remoteFamily", remoteAddress.getAddress() instanceof Inet6Address ? "IPv6" : "IPv4");

        infoParams.putMap("connection", connectionParams);
        eventParams.putMap("info", infoParams);

        sendEvent("connection", eventParams);
    }

    public TcpSocketClient getTcpClient(final int id) {
        TcpSocket socket = socketMap.get(id);
        if (socket == null) {
            throw new IllegalArgumentException(TAG + "No socket with id " + id);
        }
        if (!(socket instanceof TcpSocketClient)) {
            throw new IllegalArgumentException(TAG + "Socket with id " + id + " is not a client");
        }
        return (TcpSocketClient) socket;
    }

    private TcpSocketServer getTcpServer(final int id) {
        TcpSocket socket = socketMap.get(id);
        if (socket == null) {
            throw new IllegalArgumentException(TAG + "No socket with id " + id);
        }
        if (!(socket instanceof TcpSocketServer)) {
            throw new IllegalArgumentException(TAG + "Socket with id " + id + " is not a server");
        }
        return (TcpSocketServer) socket;
    }

    private static class CurrentNetwork {
        @Nullable
        Network network = null;

        private CurrentNetwork() {
        }

        @Nullable
        private Network getNetwork() {
            return network;
        }

        private void setNetwork(@Nullable final Network network) {
            this.network = network;
        }
    }

  ///////////////////////////////////////////////Native Serial Queue /////////////////////////////////////////////////////////
  ///////////////////////////////////////////////Native Serial Queue /////////////////////////////////////////////////////////

  /**
  ///////////////////////////////////////////////Native Serial Queue /////////////////////////////////////////////////////////
  ///////////////////////////////////////////////Native Serial Queue /////////////////////////////////////////////////////////

  /**
   * Start the queue processor - single 50ms interval system
   */
  private void startQueueProcessor() {
    android.util.Log.i(TAG, "Starting native serial queue processor with 50ms intervals");

    // Start continuous queue processing every 50ms
    queueScheduler.scheduleAtFixedRate(() -> {
      processNextCommand();
    }, 0, COMMAND_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  /**
   * Process next command from queue if available
   */
  private void processNextCommand() {
    if (isProcessing || isPaused) {
      return;
    }

    CommandItem item = nativeQueue.poll();
    if (item == null) {
      return; // Queue is empty
    }

    isProcessing = true;
    try {
      // Execute command immediately (50ms delay handled by scheduler)
      executeCommand(item);
    } finally {
      isProcessing = false;
    }
  }

  /**
   * Add command to native queue with priority
   */
  private void addToNativeQueue(String deviceName, byte[] command, int priority, String functionCaller, int retryCount) {
    if (deviceName == null || deviceName.isEmpty()) {
      android.util.Log.w(TAG, "No device connected, dropping command: " + functionCaller);
      return;
    }

    if (isPaused) {
      android.util.Log.w(TAG, "Queue is paused, command not added: " + functionCaller);
      return;
    }

    if (nativeQueue.size() >= MAX_QUEUE_SIZE) {
      android.util.Log.w(TAG, "Queue full, dropping command: " + functionCaller);
      return;
    }

    CommandItem item = new CommandItem(deviceName, command, priority, functionCaller, retryCount);
    boolean added = nativeQueue.offer(item);

    if (added) {
      android.util.Log.d(TAG, "Added to queue: " + functionCaller + " (priority=" + priority + ", size=" + nativeQueue.size() + ")");
    } else {
      android.util.Log.w(TAG, "Failed to add command to queue: " + functionCaller);
    }
  }

  /**
   * Execute a single command
   */
  private void executeCommand(CommandItem item) {
    android.util.Log.d(TAG, "Executing command: " + item.functionCaller);

    // Check if thread pools are available before executing
    if (usbWriteExecutor == null || usbWriteExecutor.isShutdown() || queueScheduler == null || queueScheduler.isShutdown()) {
      android.util.Log.w(TAG, "Cannot execute command - thread pools are not available: " + item.functionCaller);
      return;
    }

    // Execute USB write on separate thread with timeout handling
    Future<?> writeTask = usbWriteExecutor.submit(() -> {
      try {
        writeSerialportBytes(item.deviceName, item.command);
        android.util.Log.i(TAG, "writeOnClick," + item.deviceName + "," +
          java.util.Arrays.toString(item.command) + "," + item.functionCaller);
      } catch (Exception e) {
        android.util.Log.e(TAG, "USB write failed: " + e.getMessage(), e);
        throw new RuntimeException("USB write failed", e);
      }
    });

    try {
      // Handle timeout and retry logic on scheduler
      queueScheduler.schedule(() -> {
      try {
        // Check if write task completed within timeout
        writeTask.get(USB_WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
        android.util.Log.w(TAG, "USB write timeout after " + USB_WRITE_TIMEOUT_MS + "ms: " + item.functionCaller);
        writeTask.cancel(true);

        // FIRE-AND-FORGET: No retries on USB write timeout
        android.util.Log.w(TAG, "USB write timeout - fire-and-forget mode (no retry): " + item.functionCaller);
      } catch (Exception e) {
        // FIRE-AND-FORGET: No retries on USB write errors
        android.util.Log.w(TAG, "USB write error - fire-and-forget mode (no retry): " + item.functionCaller +
                          " Error: " + e.getMessage());
      }
    }, USB_WRITE_TIMEOUT_MS + 10, TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.RejectedExecutionException e) {
      android.util.Log.w(TAG, "Failed to schedule timeout handler - scheduler rejected task: " + e.getMessage());
      // Cancel the write task if we can't monitor its timeout
      writeTask.cancel(true);
    }

    // Setup retry timeout for non-read/non-reset commands
    if (!isReadCommand(item.command) && !isResetCommand(item.command)) {
      android.util.Log.d(TAG, "📝 Command needs timeout - scheduling for: " + item.commandKey + " (function: " + item.functionCaller + ")");
      scheduleRetryTimeout(item);
    } else {
      android.util.Log.d(TAG, "⏭️ Skipping timeout for read/reset command: " + item.commandKey + " (function: " + item.functionCaller + ")");
    }
  }

  /**
   * Schedule retry timeout for a command
   */
  private void scheduleRetryTimeout(CommandItem item) {
    String key = item.commandKey;

    android.util.Log.d(TAG, "🔄 Scheduling retry timeout for command: " + key + " (function: " + item.functionCaller + ")");

    // Cancel existing timeout
    ScheduledFuture<?> existingTimeout = retryTimeouts.get(key);
    if (existingTimeout != null) {
      existingTimeout.cancel(false);
      android.util.Log.d(TAG, "Cancelled existing timeout for: " + key);
    }

    // Check if scheduler is available before scheduling retry timeout
    if (queueScheduler == null || queueScheduler.isShutdown()) {
      android.util.Log.w(TAG, "Cannot schedule retry timeout - scheduler is not available");
      return;
    }

    try {
      // Log-read commands don't use timeout/retry - they rely on ACK protocol and 5-minute fallback
      // All other commands use standard 1-second timeout with 3 retries
      int timeoutMs = RETRY_TIMEOUT_MS;

      // Schedule new retry timeout
      ScheduledFuture<?> timeout = queueScheduler.schedule(() -> {
        handleRetryTimeout(item);
      }, timeoutMs, TimeUnit.MILLISECONDS);

      retryTimeouts.put(key, timeout);
      android.util.Log.d(TAG, "✅ Timeout stored successfully for: " + key + " (timeout in " + timeoutMs + "ms)");
    } catch (java.util.concurrent.RejectedExecutionException e) {
      android.util.Log.w(TAG, "Failed to schedule retry timeout - scheduler rejected task: " + e.getMessage());
    }
  }

  /**
   * Handle retry timeout for a command
   */
  private void handleRetryTimeout(CommandItem item) {
    // Special handling for reset log commands
    if (isResetLogCommand(item.command)) {
      android.util.Log.i(TAG, "Reset log command timeout - not retrying to prevent corruption");
      ScheduledFuture<?> removedTimeout = retryTimeouts.remove(item.commandKey);
      if (removedTimeout != null) {
        removedTimeout.cancel(false);
        android.util.Log.d(TAG, "🔄 Reset log command timeout cleared: " + item.commandKey);
      }
      return;
    }

    // Special handling for log-read commands - never retry
    // Log collection relies on ACK protocol and 5-minute fallback timeout
    // Retrying log commands causes duplicate data and queue flooding
    if (isLogReadCommand(item.command)) {
      android.util.Log.w(TAG, "⏭️ Log-read command timeout - not retrying (ACK protocol will handle): " + item.functionCaller);
      ScheduledFuture<?> removedTimeout = retryTimeouts.remove(item.commandKey);
      if (removedTimeout != null) {
        removedTimeout.cancel(false);
        android.util.Log.d(TAG, "🔄 Log-read command timeout cleared: " + item.commandKey);
      }
      return;
    }

    if (item.retryCount < MAX_RETRIES) {
      android.util.Log.w(TAG, "Retrying command: " + item.functionCaller +
                        " (" + (item.retryCount + 1) + "/" + MAX_RETRIES + ")");
      addToNativeQueue(item.deviceName, item.command, PRIORITY_FRONT,
                      item.functionCaller, item.retryCount + 1);
    } else {
      android.util.Log.e(TAG, "Command failed after " + MAX_RETRIES +
                        " retries: " + item.functionCaller);
    }

    ScheduledFuture<?> removedTimeout = retryTimeouts.remove(item.commandKey);
    if (removedTimeout != null) {
      removedTimeout.cancel(false);
      android.util.Log.d(TAG, "🔄 Command retry timeout cleared: " + item.commandKey);
    }
  }

  /**
   * Remove commands with matching key
   */
  private void removeCommandsByKey(String deviceName, String commandKey) {
    if (deviceName == null || commandKey == null) return;
    nativeQueue.removeIf(item ->
      deviceName.equals(item.deviceName) && commandKey.equals(item.commandKey) &&
      android.util.Log.d(TAG, "Removed duplicate command: " + commandKey) == 0
    );
  }

  /**
   * Helper method to check if command is a read command
   */
  private boolean isReadCommand(byte[] command) {
    if (command.length < 5) return false;
    for (int i = 0; i < 5; i++) {
      if (command[i] != READ_COMMAND_SIG[i]) return false;
    }
    return true;
  }

  /**
   * Helper method to check if command is reset command
   */
  private boolean isResetCommand(byte[] command) {
    return java.util.Arrays.equals(command, RESET_COMMAND_SIG);
  }

  /**
   * Helper method to check if command is reset log command
   */
  private boolean isResetLogCommand(byte[] command) {
    return command.length >= 4 && command[3] == 73 && command[4] == 1;
  }

  /**
   * Check if command is a log-read command
   * Log-read commands: [102, 0, location, location, location] (5 bytes, starts with 0x66 = 102)
   */
  private boolean isLogReadCommand(byte[] command) {
    return command.length == 5 && command[0] == 102;
  }

  /**
   * Convert ReadableArray to byte array
   */
  private byte[] readableArrayToByteArray(ReadableArray array) {
    byte[] bytes = new byte[array.size()];
    for (int i = 0; i < array.size(); i++) {
      bytes[i] = (byte) array.getInt(i);
    }
    return bytes;
  }

  // ============== React Native Interface Methods ==============

  @ReactMethod
  public void addToQueue(ReadableArray command, String functionCaller) {
    byte[] cmdBytes = readableArrayToByteArray(command);
    addToNativeQueue(heartbeatDevice, cmdBytes, PRIORITY_NORMAL, functionCaller, 0);
  }

  @ReactMethod
  public void addToQueueFront(ReadableArray command, String functionCaller) {
    byte[] cmdBytes = readableArrayToByteArray(command);
    addToNativeQueue(heartbeatDevice, cmdBytes, PRIORITY_FRONT, functionCaller, 0);
  }

  @ReactMethod
  public void addToQueueFrontReplace(ReadableArray command, String functionCaller) {
    byte[] cmdBytes = readableArrayToByteArray(command);
    String device = heartbeatDevice != null ? heartbeatDevice : "";
    String commandKey = new CommandItem(device, cmdBytes, 0, "", 0).commandKey;

    removeCommandsByKey(device, commandKey);
    addToNativeQueue(heartbeatDevice, cmdBytes, PRIORITY_REPLACE, functionCaller, 0);
  }

  @ReactMethod
  public void writeOnClick(ReadableArray command, String functionCaller) {
    addToQueueFront(command, functionCaller);
  }

  @ReactMethod
  public void writeOnClickReplace(ReadableArray command, String functionCaller) {
    addToQueueFrontReplace(command, functionCaller);
  }

  /** True when the connected PG firmware (major.minor) is at or beyond the train-back cut-in version. */
  private boolean isTrainBackSupported() {
    if (!packetIntegrityValidator.isFwKnown()) return false;
    int major = packetIntegrityValidator.getCachedFwMajor();
    int minor = packetIntegrityValidator.getCachedFwMinor();
    if (major != TRAIN_BACK_MIN_FW_MAJOR) return major > TRAIN_BACK_MIN_FW_MAJOR;
    return minor >= TRAIN_BACK_MIN_FW_MINOR;
  }

  // Skip backward one train (protocol location 2118). No-ops on firmware below the
  // train-back cut-in version — the JS layer also gates the button via the
  // "trainBackSupported" field on the device status stream, this is defense in depth.
  @ReactMethod
  public void trainBack() {
    if (!isTrainBackSupported()) {
      android.util.Log.w(TAG, "trainBack() ignored - unsupported on current PG firmware");
      return;
    }
    addToNativeQueue(heartbeatDevice, TRAIN_BACK_COMMAND, PRIORITY_FRONT, "train back", 0);
  }

  // Skip forward one train (protocol location 2118). Unlike trainBack(), this has always
  // worked and is not firmware-gated.
  @ReactMethod
  public void trainForward() {
    addToNativeQueue(heartbeatDevice, TRAIN_FORWARD_COMMAND, PRIORITY_FRONT, "train forward", 0);
  }

  @ReactMethod
  public void writeReadCommand(ReadableArray command, String functionCaller) {
    int readCommandCount = 0;
    for (CommandItem item : nativeQueue) {
      if (isReadCommand(item.command)) {
        readCommandCount++;
        if (readCommandCount >= 2) break;
      }
    }

    if (readCommandCount < 2) {
      addToQueue(command, functionCaller);
    } else {
      android.util.Log.d(TAG, "Ignored read command - already 2 in queue");
    }
  }

  @ReactMethod
  public void clearQueue() {
    nativeQueue.clear();
    isProcessing = false;

    // Clear all retry timeouts
    for (ScheduledFuture<?> timeout : retryTimeouts.values()) {
      timeout.cancel(false);
    }
    retryTimeouts.clear();

    android.util.Log.i(TAG, "Native queue cleared");
  }

  @ReactMethod
  public void pauseQueue() {
    isPaused = true;
    android.util.Log.i(TAG, "Native queue paused");
  }

  @ReactMethod
  public void resumeQueue() {
    isPaused = false;
    android.util.Log.i(TAG, "Native queue resumed");
  }

  @ReactMethod
  public void clearCommandTimeout(ReadableArray command) {
    byte[] cmdBytes = readableArrayToByteArray(command);
    String key = new CommandItem("", cmdBytes, 0, "", 0).commandKey;

    ScheduledFuture<?> timeout = retryTimeouts.remove(key);
    if (timeout != null) {
      timeout.cancel(false);
      android.util.Log.d(TAG, "✅ JS command timeout cleared successfully: " + key);
    } else {
      android.util.Log.w(TAG, "❌ JS clearCommandTimeout - No timeout found for key: " + key +
                        " (Active timeouts: " + retryTimeouts.size() + ")");
    }
  }

  /**
   * Cleanup method to properly shutdown resources
   */
  private void cleanup() {
    android.util.Log.i(TAG, "🧹 CLEANUP: Starting module cleanup");

    try {
      // CRITICAL: Clean up all active log collections
      for (Map.Entry<String, Boolean> entry : deviceLoggingActive.entrySet()) {
        if (entry.getValue()) {
          String device = entry.getKey();
          android.util.Log.w(TAG, "🧹 CLEANUP: Active log collection found for: " + device);
          cleanupLogCollectionState(device, true);
        }
      }
    } catch (Exception e) {
      android.util.Log.e(TAG, "❌ CLEANUP: Error cleaning up log collections: " + e.getMessage());
    }

    // FIX TMS-APP-56: Stop all USB connections BEFORE shutting down executors
    // This prevents race condition where USB read callbacks try to submit tasks to terminated executor
    if (serialPorts != null && !serialPorts.isEmpty()) {
      android.util.Log.i(TAG, "🧹 CLEANUP: Stopping " + serialPorts.size() + " active USB connection(s)");
      // Create copy to avoid ConcurrentModificationException since stopConnection modifies the map
      java.util.List<String> deviceNames = new java.util.ArrayList<>(serialPorts.keySet());
      for (String deviceName : deviceNames) {
        try {
          android.util.Log.d(TAG, "🧹 CLEANUP: Stopping USB connection for: " + deviceName);
          stopConnection(deviceName);
        } catch (Exception e) {
          android.util.Log.e(TAG, "❌ CLEANUP: Error stopping connection for " + deviceName + ": " + e.getMessage());
        }
      }
      android.util.Log.i(TAG, "✅ CLEANUP: All USB connections stopped");
    }

    // Stop heartbeat
    stopHeartbeat();

    // Clear and shutdown queue
    clearQueue();
    isPaused = true;

    // Shutdown USB write executor
    if (usbWriteExecutor != null && !usbWriteExecutor.isShutdown()) {
      usbWriteExecutor.shutdown();
      try {
        if (!usbWriteExecutor.awaitTermination(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          android.util.Log.w(TAG, "USB write executor did not terminate gracefully, forcing shutdown");
          usbWriteExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        android.util.Log.w(TAG, "USB write executor cleanup interrupted, forcing shutdown");
        usbWriteExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    // Shutdown USB processing executor
    if (usbProcessingExecutor != null && !usbProcessingExecutor.isShutdown()) {
      usbProcessingExecutor.shutdown();
      try {
        if (!usbProcessingExecutor.awaitTermination(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          android.util.Log.w(TAG, "USB processing executor did not terminate gracefully, forcing shutdown");
          usbProcessingExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        android.util.Log.w(TAG, "USB processing executor cleanup interrupted, forcing shutdown");
        usbProcessingExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    // FIX #9: Clean up all per-device packet buffers on module shutdown
    if (devicePacketBuffers != null && !devicePacketBuffers.isEmpty()) {
      int bufferCount = devicePacketBuffers.size();
      devicePacketBuffers.clear();
      android.util.Log.d(TAG, "🧹 Cleared " + bufferCount + " device packet buffer(s) on module cleanup");
    }

    // Shutdown scheduler with timeout
    if (queueScheduler != null && !queueScheduler.isShutdown()) {
      queueScheduler.shutdown();
      try {
        // Wait for termination for up to specified timeout
        if (!queueScheduler.awaitTermination(CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          android.util.Log.w(TAG, "Queue scheduler did not terminate gracefully, forcing shutdown");
          queueScheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        android.util.Log.w(TAG, "Cleanup interrupted, forcing shutdown");
        queueScheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    android.util.Log.i(TAG, "Cleanup completed");
  }

  // ============== Heartbeat Integration ==============

  @ReactMethod
  public void startHeartbeat(String deviceName) {
    // MODIFIED: Heartbeat runs continuously - switches to lightweight command during log collection
    android.util.Log.i(TAG, "Starting native heartbeat for device: " + deviceName);

    // Check if thread pools are available before attempting to schedule tasks
    if (queueScheduler == null || queueScheduler.isShutdown()) {
      android.util.Log.w(TAG, "Cannot start heartbeat - scheduler is not available or has been shut down");
      return;
    }

    // Check if already running for this device
    if (heartbeatDevice != null && heartbeatDevice.equals(deviceName)) {
      android.util.Log.d(TAG, "💚 Heartbeat already running for: " + deviceName);
      return;
    }

    // Turn camera ON if reconnecting while in control mode
    if (controlMode != null) {
      android.util.Log.d(TAG, "USB reconnect in control mode - turning camera ON");
      addToNativeQueue(deviceName, CAMERA_ON_COMMAND, PRIORITY_NORMAL, "camera_on_reconnect", 0);
    }

    stopHeartbeat();

    if (!serialPorts.containsKey(deviceName)) {
      android.util.Log.w(TAG, "Cannot start heartbeat - device not connected: " + deviceName);
      return;
    }

    heartbeatDevice = deviceName;

    try {
      // Schedule heartbeat to ADD commands to queue every 250ms
      // CRITICAL: 0ms initial delay to prevent USB hub reset after log collection
      // The device firmware triggers USB reset if no commands received for ~700ms
      // By starting immediately (0ms), we ensure first heartbeat within 0-25ms
      heartbeatTimer = queueScheduler.scheduleAtFixedRate(() -> {
        String currentDevice = heartbeatDevice;
        if (currentDevice != null && serialPorts.containsKey(currentDevice)) {

          // Always send full heartbeat - priority queue ensures no interference
          // Log commands (priority 3) automatically execute before heartbeats (priority 1)
          addToNativeQueue(currentDevice, HEARTBEAT_COMMAND, PRIORITY_HEARTBEAT, "heartbeat", 0);
        }
      }, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

      android.util.Log.i(TAG, "Native heartbeat started - first command immediate, then every " + HEARTBEAT_INTERVAL_MS + "ms");
    } catch (java.util.concurrent.RejectedExecutionException e) {
      android.util.Log.w(TAG, "Failed to start heartbeat - scheduler rejected task: " + e.getMessage());
    }
  }

  @ReactMethod
  public void stopHeartbeat() {
    if (heartbeatTimer != null && !heartbeatTimer.isCancelled()) {
      heartbeatTimer.cancel(false);
      android.util.Log.i(TAG, "Stopped native heartbeat for device: " + heartbeatDevice);
    }
    heartbeatTimer = null;
    heartbeatDevice = null;
  }

  ///////////////////////////////////////////////Gateway /////////////////////////////////////////////////////////
  ///////////////////////////////////////////////Gateway /////////////////////////////////////////////////////////

  @ReactMethod
  public void appBus2DeviceNamePut(Integer appBus, String deviceName) {
    appBus2DeviceName.put(appBus, deviceName);
  }

  ///////////////////////////////////////////////Native Data Processing /////////////////////////////////////////////////////////
  ///////////////////////////////////////////////Native Data Processing /////////////////////////////////////////////////////////

  /**
   * DEPRECATED: Old buffer-based packet assembly (replaced by PacketAssembler)
   * Keeping class name for backward compatibility, but using new implementation internally
   */
  public static class NativePacketBuffer {
    private final PacketAssembler assembler = new PacketAssembler();

    /**
     * Append incoming bytes and extract any complete packets
     * @param newBytes Raw bytes from USB
     * @return List of complete packets ready for processing
     */
    public synchronized List<byte[]> appendAndExtractPackets(byte[] newBytes) {
      List<Packet> packets = assembler.feedBytes(newBytes);

      // Convert to byte arrays for compatibility
      List<byte[]> completePackets = new ArrayList<>();
      for (Packet packet : packets) {
        completePackets.add(packet.toByteArray());
      }

      return completePackets;
    }

    /**
     * Enable/disable CRC trailer framing on the underlying assembler.
     */
    public synchronized void setCrcEnabled(boolean enabled) {
      assembler.setCrcEnabled(enabled);
    }

    /**
     * Clear the assembler state (for error recovery)
     */
    public synchronized void clear() {
      android.util.Log.d(TAG, "Clearing packet assembler");
      assembler.reset();
    }

    /**
     * Get current assembler status for debugging
     */
    public synchronized String getBufferStatus() {
      return assembler.getStateDebugInfo();
    }
  }

  /**
   * Process incoming USB bytes through native packet assembly
   * @param deviceName The USB device name
   * @param rawBytes Raw bytes from USB callback
   */
  /** True when the PG firmware (major.minor) is at or beyond the CRC cut-in version. */
  private boolean fwSupportsCrc(int major, int minor) {
    if (major != CRC_MIN_FW_MAJOR) return major > CRC_MIN_FW_MAJOR;
    return minor >= CRC_MIN_FW_MINOR;
  }

  /** Whether CRC framing/validation is active for this device (false until fw is known). */
  private boolean isCrcEnabled(String deviceName) {
    return Boolean.TRUE.equals(crcEnabledByDevice.get(deviceName));
  }

  /**
   * Keep a device's packet assembler framing in sync with the CRC decision for its PG
   * firmware. Called at the start of each RX batch (before extraction) so framing and
   * validation never disagree mid-batch.
   *
   * The decision itself (does this firmware speak CRC?) is made once per connection and
   * cached in crcEnabledByDevice; the self-test and log line fire only on that first
   * decision. But the decision must be re-applied to the assembler on EVERY batch, because
   * the per-device buffer can be destroyed and recreated mid-connection (e.g. the buffer is
   * removed when a log-collection completes) and a fresh assembler defaults to CRC-off. If
   * we only pushed the flag on the first decision, a recreated buffer would frame 58-byte
   * status packets while V0.16+ firmware keeps sending 60-byte (58 + 2 CRC) frames — a
   * permanent 2-byte desync that fails every CRC and drops all status until a reconnect.
   * setCrcEnabled() logs only on an actual change, so re-applying every batch is silent in
   * steady state. crcEnabledByDevice is cleared on disconnect so a reconnected/different PG
   * re-evaluates.
   */
  private void updateCrcEnablement(String deviceName, NativePacketBuffer buffer) {
    Boolean enabled = crcEnabledByDevice.get(deviceName);
    if (enabled == null) {
      // First batch for this connection: decide from the cached firmware version.
      if (!packetIntegrityValidator.isFwKnown()) return;      // need a clean status packet first
      int major = packetIntegrityValidator.getCachedFwMajor();
      int minor = packetIntegrityValidator.getCachedFwMinor();
      enabled = fwSupportsCrc(major, minor);
      crcEnabledByDevice.put(deviceName, enabled);
      if (enabled) {
        // Runtime proof the CRC algorithm produces the documented check value on this build.
        boolean selfTest = Crc16.selfTestPasses();
        if (!selfTest) {
          android.util.Log.e(TAG, "CRC SELF-TEST FAILED — algorithm is broken on this build; " +
              "every frame will fail validation!");
        }
        android.util.Log.i(TAG, "CRC ENABLED for " + deviceName + " — PG firmware V" + major + "." +
            minor + " (self-test " + (selfTest ? "OK" : "FAILED") + ")");
      } else {
        android.util.Log.i(TAG, "CRC disabled (legacy PG) for " + deviceName +
            " — PG firmware V" + major + "." + minor);
      }
    }
    // Every batch: re-derive the live buffer's framing from the authoritative decision so a
    // recreated/reset buffer cannot silently drift out of sync with the firmware.
    buffer.setCrcEnabled(enabled);
  }

  private void processNativePackets(String deviceName, byte[] rawBytes) {
    try {
      // FIX #9: Get or create per-device buffer for parallel processing
      // Each device has its own buffer, eliminating thread contention
      NativePacketBuffer buffer = devicePacketBuffers.computeIfAbsent(
          deviceName,
          k -> new NativePacketBuffer()
      );

      // Decide CRC enablement before extracting, so the assembler's framing flag and
      // the router's validation are consistent for every packet in this batch.
      updateCrcEnablement(deviceName, buffer);

      // Use device-specific packet buffer to assemble complete packets
      List<byte[]> completePackets = buffer.appendAndExtractPackets(rawBytes);

      // Process each complete packet immediately
      for (byte[] packet : completePackets) {
        android.util.Log.d(TAG, "Processing native packet: " + packet.length + " bytes");
        processPacketDirectly(deviceName, packet);
      }

      // Log buffer status for debugging
      if (completePackets.size() > 0) {
        android.util.Log.d(TAG, "Native buffer status for " + deviceName + ": " + buffer.getBufferStatus());
      }
    } catch (Exception e) {
      android.util.Log.e(TAG, "Error in native packet processing: " + e.getMessage(), e);
      // Clear device-specific buffer on error to prevent corruption
      NativePacketBuffer buffer = devicePacketBuffers.get(deviceName);
      if (buffer != null) {
        buffer.clear();
      }
    }
  }

  /**
   * Process a complete packet directly in native code
   * @param deviceName The USB device name
   * @param packet Complete packet bytes
   */
  /**
   * Industry-standard packet routing based on protocol location field
   * Implements packet type detection and routing to appropriate processors
   */
  private void processPacketDirectly(String deviceName, byte[] packet) {
    try {
      // Industry standard: validate minimum packet size
      if (packet.length < 5) {
        android.util.Log.w(TAG, "Packet too short: " + packet.length + " bytes (minimum 5 required)");
        emitPacketProcessingError(deviceName, "PACKET_TOO_SHORT", packet);
        return;
      }

      // Industry standard: validate packet header (102 for read responses, 170 for write acknowledgments)
      byte header = packet[0];
      if (header != 102 && header != (byte) 170) {
        android.util.Log.w(TAG, "Invalid packet header: " + header + " (expected 102 or 170)");
        emitPacketProcessingError(deviceName, "INVALID_HEADER", packet);
        return;
      }

      // CRC validation. Per the AMPA tester, only READ responses (0x66) carry a CRC
      // trailer; the write ACK (0xAA) carries none and is handled below as a plain ack.
      if (isCrcEnabled(deviceName) && header == 102) {
        // Too short to contain the trailer — cannot be validated, so drop it.
        if (packet.length < 5 + CRC_TRAILER_LEN) {
          android.util.Log.e(TAG, "CRC-enabled read response too short to contain trailer (" +
                            packet.length + " bytes) — dropping");
          return;
        }
        int n = packet.length;
        int crcCalc = Crc16.compute(packet, 0, n - CRC_TRAILER_LEN);
        int crcRecv = ((packet[n - CRC_TRAILER_LEN] & 0xFF) << 8) | (packet[n - 1] & 0xFF);
        if (crcCalc != crcRecv) {
          // Mismatch → ignore the response; the next read (~250ms) supersedes it.
          int loc = convertThreeBytesToNumber(packet, 1);
          android.util.Log.e(TAG, "CRC for request read@0x" + String.format("%06X", loc) +
                            " does not match (calc=0x" + String.format("%04X", crcCalc) +
                            ", recv=0x" + String.format("%04X", crcRecv) + ")");
          return;
        }

        // Valid CRC — strip the trailer and continue with normal routing.
        packet = java.util.Arrays.copyOf(packet, n - CRC_TRAILER_LEN);
      }

      // Handle write acknowledgment packets (header 170)
      if (header == (byte) 170) {
        android.util.Log.d(TAG, "Processing write acknowledgment - Length: " + packet.length +
                          " bytes, Device: " + deviceName);
        processWriteAcknowledgment(deviceName, packet);
        return;
      }

      // Handle read response packets (header 102)
      // Industry standard: parse location field from bytes 1-3 (24-bit address)
      int location = convertThreeBytesToNumber(packet, 1);
      android.util.Log.d(TAG, "Processing read response - Length: " + packet.length +
                        " bytes, Location: " + location + ", Device: " + deviceName);

      // Industry standard: route packets based on protocol location field
      if (location == 0) {
        // Device status packets (standard telemetry)
        processDeviceStatusPacket(deviceName, packet);
      } else if (location >= 2200) {
        // Hardware pulse data packets (high-frequency sensor data)
        processHardwarePulsePacket(deviceName, packet);
      } else if (location == 2103) {
        // Sequence response packets (command acknowledgment)
        processSequenceResponsePacket(deviceName, packet);
      } else {
        // Unknown packet type - log for protocol analysis
        android.util.Log.w(TAG, "Unknown packet location: " + location +
                          " (length: " + packet.length + ")");
        emitPacketProcessingError(deviceName, "UNKNOWN_LOCATION", packet);
      }

    } catch (Exception e) {
      android.util.Log.e(TAG, "Critical error in packet processing: " + e.getMessage(), e);
      emitPacketProcessingError(deviceName, "PROCESSING_EXCEPTION", packet);
    }
  }

  /**
   * Industry standard: convert three bytes to number (24-bit address parsing)
   * Used for packet location field extraction
   */
  private int convertThreeBytesToNumber(byte[] data, int offset) {
    if (data.length < offset + 3) {
      throw new IllegalArgumentException("Insufficient data for three-byte conversion");
    }
    return ((data[offset] & 0xFF) << 16) |
           ((data[offset + 1] & 0xFF) << 8) |
           (data[offset + 2] & 0xFF);
  }

  /**
   * Process device status packets (location = 0)
   * Standard telemetry data processing with structured event emission
   */
  private void processDeviceStatusPacket(String deviceName, byte[] packet) {
    try {
      // Note: Heartbeat responses are handled by processStatusPacket, not here

      // Validate packet size for device status. A complete RevG status packet is 58 bytes
      // (5-byte header + 53 data) — needed for resistor temp (doc 51) + fan speed (doc 52).
      if (packet.length < 58) {
        android.util.Log.w(TAG, "Device status packet too short: " + packet.length + " bytes (need 58)");
        return;
      }

      android.util.Log.d(TAG, "Processing device status packet (" + packet.length + " bytes)");

      // Validate integrity BEFORE constructing DeviceStatusData.
      // DeviceStatusData updates the EMA temperature cache in its constructor, so building it
      // from a corrupt packet poisons the cache and causes decaying bad values across many
      // subsequent valid heartbeats. Bail here instead so the EMA is never touched.
      PacketIntegrityValidator.Result integrityResult = packetIntegrityValidator.validate(packet);
      if (integrityResult.failed) {
        WritableMap statusParams = Arguments.createMap();
        statusParams.putString("deviceName", deviceName);
        statusParams.putString("eventType", "DEVICE_STATUS");
        WritableMap dataMap = Arguments.createMap();
        dataMap.putBoolean("integrityFailed", true);
        dataMap.putInt("integrityScore", integrityResult.score);
        dataMap.putString("integrityViolations", integrityResult.violations);
        statusParams.putMap("data", dataMap);
        eventEmit("onNativeDeviceStatus", statusParams);
        return;
      }

      // Parse device status using existing native data structure with temperature smoothing.
      // Only reached for packets that passed integrity validation.
      DeviceStatusData deviceStatus = new DeviceStatusData(packet, deviceName, temperatureCache);

      // 🛡️ SAFETY CHECK: Reject hardware enable if not on control screen
      if (deviceStatus.magVentureEnabled && controlMode == null) {
        android.util.Log.w(TAG, "⚠️ HARDWARE BUTTON PRESSED - REJECTING (not on control screen)");
        addToNativeQueue(deviceName, PG_DISABLE_COMMAND, PRIORITY_FRONT, "reject_hardware_enable", 0);
        return; // Skip event emission - don't notify JS layer
      }

      // 🛡️ SAFETY CHECK: Disable PG if coil is disconnected
      if (deviceStatus.coilDisconnected) {
        android.util.Log.e(TAG, "🔌 COIL DISCONNECTED - Disabling PG for safety");
        addToNativeQueue(deviceName, PG_DISABLE_COMMAND, PRIORITY_FRONT, "coil_disconnected_disable", 0);
        // Continue to emit event so JS layer can show error toast
      }

      // Emit structured device status event
      WritableMap statusParams = Arguments.createMap();
      statusParams.putString("deviceName", deviceName);
      statusParams.putString("eventType", "DEVICE_STATUS");
      WritableMap dataMap = deviceStatus.toWritableMap();

      // Validate timestamp — returns last valid value on garbled packets
      long validatedTimestamp = timestampValidator.validate(deviceStatus.timestamp, deviceStatus.treatmentStatus);
      if (validatedTimestamp >= 0) {
        dataMap.putDouble("timestamp", validatedTimestamp);
      }

      // Add integrity check results to the existing data map
      dataMap.putInt("integrityScore", integrityResult.score);
      dataMap.putBoolean("integrityFailed", integrityResult.failed);
      dataMap.putString("integrityViolations", integrityResult.violations);

      statusParams.putMap("data", dataMap);
      eventEmit("onNativeDeviceStatus", statusParams);

      // Auto-ramp: check timeline keyframes; emit if a manual override was detected
      boolean autoRampOverride = autoRampEngine.checkAndApply(deviceName, deviceStatus.timestamp, deviceStatus.treatmentStatus, deviceStatus.mso, deviceStatus.lastPulseIndex, deviceStatus.trainsInSequence);
      if (autoRampOverride) {
        WritableMap overrideParams = Arguments.createMap();
        overrideParams.putString("deviceName", deviceName);
        eventEmit("onAutoRampManualOverride", overrideParams);
      }

      // Emit power status event
      WritableMap powerParams = Arguments.createMap();
      powerParams.putString("deviceName", deviceName);
      powerParams.putString("eventType", "POWER_STATUS");
      powerParams.putBoolean("pulseGeneratorOn", deviceStatus.pulseGeneratorOn);
      powerParams.putInt("voltage", deviceStatus.voltage);
      powerParams.putDouble("chargeVoltage", deviceStatus.chargeVoltage);
      eventEmit("onNativePowerStatus", powerParams);

      // Emit play status event with treatmentStatus to match SerialUtils.ts
      WritableMap playParams = Arguments.createMap();
      playParams.putString("deviceName", deviceName);
      playParams.putString("eventType", "PLAY_STATUS");
      playParams.putInt("treatmentStatus", deviceStatus.treatmentStatus);
      playParams.putBoolean("isPlayingSession", deviceStatus.isPlayingSession);
      playParams.putBoolean("isRecordingSession", deviceStatus.isRecordingSession);
      playParams.putInt("playingTreatmentIndex", deviceStatus.playingTreatmentIndex);
      eventEmit("onNativePlayStatus", playParams);

      // PHASE 1: Auto-resume validation for device reconnection during log collection
      // CRITICAL FIX: Don't check device name equality because USB hub path changes
      // (e.g., /dev/bus/usb/001/012 -> /dev/bus/usb/001/016) during reconnection.
      // Instead, rely on wasCollectingLogs flag and pulse index validation.
      if (wasCollectingLogs) {
        android.util.Log.i(TAG, "🔄 Auto-resume validation: Device reconnected during log collection: " + deviceName +
                          " (original device was: " + lastCollectionDevice + ")");

        // Validate device memory state using lastPulseIndex
        if (deviceStatus.lastPulseIndex == lastExpectedPulseIndex && lastExpectedPulseIndex > 0) {
          android.util.Log.i(TAG, "✅ Auto-resume validation PASSED: lastPulseIndex matches (" +
                            deviceStatus.lastPulseIndex + "), resuming log collection");

          // Cancel validation timeout for BOTH old and new device names
          Runnable timeoutRunnable = deviceValidationTimeouts.remove(lastCollectionDevice);
          if (timeoutRunnable != null) {
            timeoutHandler.removeCallbacks(timeoutRunnable);
          }
          timeoutRunnable = deviceValidationTimeouts.remove(deviceName);
          if (timeoutRunnable != null) {
            timeoutHandler.removeCallbacks(timeoutRunnable);
          }

          // Clear auto-resume state
          wasCollectingLogs = false;
          String originalDevice = lastCollectionDevice; // Save for logging
          lastCollectionDevice = null;
          lastExpectedPulseIndex = 0;

          android.util.Log.i(TAG, "🔄 Resuming log collection: " + originalDevice + " -> " + deviceName);

          // Resume log collection from where we left off
          autoStartLogCollection(deviceName, deviceStatus.lastPulseIndex);
        } else {
          // Validation FAILED - device memory doesn't match expected state
          android.util.Log.e(TAG, "❌ Auto-resume validation FAILED: Expected lastPulseIndex=" +
                            lastExpectedPulseIndex + ", actual=" + deviceStatus.lastPulseIndex +
                            " (device path changed: " + lastCollectionDevice + " -> " + deviceName + ")");

          // Emit reconnect failed event
          WritableMap failParams = Arguments.createMap();
          failParams.putString("deviceName", deviceName);
          failParams.putInt("expectedPulseIndex", lastExpectedPulseIndex);
          failParams.putInt("actualPulseIndex", deviceStatus.lastPulseIndex);
          failParams.putString("reason", "pulse_index_mismatch");
          eventEmit("onLogCollectionReconnectFailed", failParams);

          // Clear auto-resume state
          wasCollectingLogs = false;
          lastCollectionDevice = null;
          lastExpectedPulseIndex = 0;

          // Cancel validation timeout for BOTH old and new device names
          Runnable timeoutRunnable = deviceValidationTimeouts.remove(lastCollectionDevice);
          if (timeoutRunnable != null) {
            timeoutHandler.removeCallbacks(timeoutRunnable);
          }
          timeoutRunnable = deviceValidationTimeouts.remove(deviceName);
          if (timeoutRunnable != null) {
            timeoutHandler.removeCallbacks(timeoutRunnable);
          }
        }
      }

      // Auto-start log collection when treatment completes (treatmentStatus = 0)
      // ✅ FIXED: Only trigger on PLAYING(1)/PAUSED(2) → IDLE(0) transition
      // ✅ FIXED: Only auto-start for TREATMENT mode, not MAPPING/MANUAL modes
      int prevTreatmentStatus = lastProcessedTreatmentStatus;
      int prevPulseIndex = lastProcessedPulseIndex;
      String currentControlMode = this.controlMode != null ? this.controlMode : "TREATMENT";

      if (deviceStatus.treatmentStatus == 0 &&
          deviceStatus.lastPulseIndex > 0 &&
          (prevTreatmentStatus == 1 || prevTreatmentStatus == 2)) {

        if ("TREATMENT".equals(currentControlMode)) {
          android.util.Log.i(TAG, "Treatment completion detected: " + deviceName +
                            " status: " + prevTreatmentStatus + " -> 0, pulses: " +
                            prevPulseIndex + " -> " + deviceStatus.lastPulseIndex);

          autoStartLogCollection(deviceName, deviceStatus.lastPulseIndex);
        } else {
          android.util.Log.i(TAG, "Sequence completion in " + currentControlMode + " mode - skipping auto-log collection: " + deviceName);
        }
      }

      // Update last processed values (device-path-independent)
      lastProcessedTreatmentStatus = deviceStatus.treatmentStatus;
      lastProcessedPulseIndex = deviceStatus.lastPulseIndex;

      // Emit MSO status event
      WritableMap msoParams = Arguments.createMap();
      msoParams.putString("deviceName", deviceName);
      msoParams.putString("eventType", "MSO_STATUS");
      msoParams.putInt("mso", deviceStatus.mso);
      msoParams.putInt("currentMso", deviceStatus.currentMso);
      eventEmit("onNativeMsoStatus", msoParams);

      android.util.Log.d(TAG, "Device status processing completed for: " + deviceName);

    } catch (Exception e) {
      android.util.Log.e(TAG, "Error processing device status packet: " + e.getMessage(), e);
      emitPacketProcessingError(deviceName, "DEVICE_STATUS_ERROR", packet);
    }
  }

  // FIX #7: Thread-safe pulse gap detection with AtomicLong
  // 4 concurrent threads access this variable - must be atomic to prevent race conditions
  private final AtomicLong lastPulseNumber = new AtomicLong(-1);

  /**
   * Process hardware pulse packets (location >= 2200)
   * With native-only acknowledgment system to prevent duplicate packet processing
   *
   * Acknowledgment Flow:
   * 1. Parse pulses from packet
   * 2. Check if we're waiting for this response (log collection mode)
   * 3. If waiting: process ONCE, update index, emit to JS, continue to next read
   * 4. If not waiting: ignore (duplicate) or emit normally (live treatment)
   */
  private void processHardwarePulsePacket(String deviceName, byte[] packet) {
    try {
      android.util.Log.d(TAG, "Processing hardware pulse packet (" + packet.length + " bytes)");

      // Industry standard: validate minimum packet size for pulse data
      if (packet.length < 39) { // 5 byte header + 34 byte minimum pulse data
        android.util.Log.w(TAG, "Hardware pulse packet too short: " + packet.length + " bytes");
        emitPacketProcessingError(deviceName, "PULSE_PACKET_TOO_SHORT", packet);
        return;
      }

      // Industry standard: parse multiple pulses from packet payload
      WritableArray pulses = Arguments.createArray();
      final int PULSE_SIZE = 34; // Industry standard: 34 bytes per pulse
      final int PULSE_DATA_OFFSET = 5; // Skip 5-byte header

      // Process each complete pulse in the packet
      for (int i = PULSE_DATA_OFFSET; i < packet.length; i += PULSE_SIZE) {
        if (i + PULSE_SIZE <= packet.length) {
          WritableMap pulse = parseHardwarePulse(packet, i);
          if (pulse != null) {
            // Pulse gap detection (thread-safe with AtomicLong)
            long pulseNum = (long) pulse.getDouble("pulseNumber");
            long prevPulse = lastPulseNumber.getAndSet(pulseNum);
            if (prevPulse != -1 && pulseNum != prevPulse + 1) {
              long missing = pulseNum - prevPulse - 1;
              android.util.Log.w(TAG, "⚠️ PULSE GAP DETECTED: Missing " + missing +
                " pulse(s) between #" + prevPulse + " and #" + pulseNum);
            }

            pulses.pushMap(pulse);
          }
        } else {
          android.util.Log.w(TAG, "Incomplete pulse data at offset " + i +
                            " (remaining: " + (packet.length - i) + " bytes)");
        }
      }

      // FIRE-AND-FORGET: Just process all incoming pulses (no acknowledgment needed)
      if (deviceLoggingActive.getOrDefault(deviceName, false)) {
        int receivedCount = pulses.size();
        android.util.Log.d(TAG, "📥 Received log response: " + receivedCount + " pulses");

        // Filter duplicates using Set-based tracking to handle out-of-order delivery
        // Initialize set if needed
        // FIX #3: Use thread-safe ConcurrentHashMap.newKeySet() instead of HashSet
        // Multiple USB processing threads (4 threads in usbProcessingExecutor pool) call
        // seenPulses.add() concurrently. HashSet is NOT thread-safe - concurrent adds
        // cause data corruption where some entries are lost, resulting in incomplete data
        // (1492/1800 = 82% instead of 100%). ConcurrentHashMap.newKeySet() is thread-safe.
        // FIX #8: Use computeIfAbsent() to fix Set initialization race condition
        // RACE CONDITION: get() + null check + create + put() is NOT atomic across threads
        // Multiple threads can create separate Set instances, overwriting each other and losing data
        Set<Integer> seenPulses = deviceSeenPulseNumbers.computeIfAbsent(
            deviceName,
            k -> ConcurrentHashMap.newKeySet()
        );

        WritableArray uniquePulses = Arguments.createArray();
        int uniqueCount = 0;
        int highestPulseNum = 0;

        for (int i = 0; i < pulses.size(); i++) {
          ReadableMap pulse = pulses.getMap(i);
          int pulseNumber = (int) pulse.getDouble("pulseNumber");

          // FIX #4: Use atomic add() instead of check-then-act race condition
          // RACE CONDITION BUG: contains() check followed by add() is NOT atomic
          // With 4 threads, multiple threads pass contains() before any calls add(),
          // causing duplicates to be emitted (2935 total vs 1800 unique) and some
          // pulses to never be tracked (1464 in Set vs 1800 expected).
          // SOLUTION: Set.add() is atomic and returns true if added, false if duplicate.
          if (seenPulses.add(pulseNumber)) {
            // New pulse - was successfully added to set
            uniquePulses.pushMap(pulse);
            uniqueCount++;
            highestPulseNum = Math.max(highestPulseNum, pulseNumber);
          } else {
            // True duplicate - was already in set
            android.util.Log.v(TAG, "⚠️ DUPLICATE: Pulse #" + pulseNumber + " already seen");
          }
        }

        // Log set size for monitoring (helps detect memory issues if set grows too large)
        android.util.Log.d(TAG, "📊 Pulse tracking: " + seenPulses.size() + " unique pulses seen");

        // FIX #2: Update deviceLogIndex for accurate completion event reporting
        // Without this, completion event always reports currentIndex=0
        deviceLogIndex.put(deviceName, seenPulses.size());

        // Emit ONLY unique pulses to JavaScript with batch sequence for ACK protocol
        if (uniqueCount > 0) {
          // FIX #6: Atomic increment to prevent race condition with 4 concurrent threads
          // RACE CONDITION: get() + increment + put() is NOT atomic - multiple threads can
          // read the same value and overwrite each other, causing duplicate batch numbers.
          // SOLUTION: Use compute() for atomic read-modify-write operation.
          // FIX #7: Start batch numbering at 0 instead of 1 to match expectedBatches calculation
          // expectedBatches = ceil(1800/7) = 257, so we need batches 0-256 (257 total)
          // Previous: (v == null ? 0 : v) + 1 produced batches 1-256 (only 256 batches)
          // Fixed: (v == null ? -1 : v) + 1 produces batches 0-256 (257 batches)
          int batchSeq = devicePulseBatchesSent.compute(deviceName, (k, v) -> (v == null ? -1 : v) + 1);

          WritableMap pulseParams = Arguments.createMap();
          pulseParams.putString("deviceName", deviceName);
          pulseParams.putString("eventType", "HARDWARE_PULSES");
          pulseParams.putArray("pulses", uniquePulses);
          pulseParams.putInt("pulseCount", uniqueCount);
          pulseParams.putInt("batchSequence", batchSeq);
          eventEmit("onNativeHardwarePulses", pulseParams);

          android.util.Log.d(TAG, "✅ Sent batch #" + batchSeq + ": " + uniqueCount + "/" + receivedCount +
                            " unique pulses, highest: #" + highestPulseNum);

          // 🔍 PULSE GAP DETECTION: Validate pulse sequence continuity within batch
          if (uniqueCount == MAX_PULSES_PER_FETCH) {  // Full batch (7 pulses)
            for (int i = 1; i < uniquePulses.size(); i++) {
              int prev = (int) uniquePulses.getMap(i - 1).getDouble("pulseNumber");
              int curr = (int) uniquePulses.getMap(i).getDouble("pulseNumber");
              if (curr != prev + 1) {
                android.util.Log.e(TAG, "🚨 PULSE GAP IN BATCH #" + batchSeq + ": Expected " + (prev + 1) +
                                  " but got " + curr + " (gap of " + (curr - prev - 1) + " pulses)");
                android.util.Log.e(TAG, "   This indicates packet assembly or hardware issue!");
              }
            }
          }
        }

        // ✅ FIX: Removed premature completion check that was causing race condition
        // The check at line 2398 (seenPulses.size() >= targetPulses) was triggering completion
        // BEFORE all packets were processed, resulting in incomplete data (1471/1800 pulses).
        // Completion is now handled ONLY by:
        // 1. ACK-based completion when JS acknowledges all batches (preferred)
        // 2. Fallback timeout (60s buffer) as safety net
        // This ensures all pulse packets are fully processed before completion.
      }
      // ✅ REMOVED: else branch that emitted pulses without batchSequence
      // Hardware documentation confirms pulse data ONLY comes from log collection
      // (Treatment Log Table, bytes 2201-172200). There is no live pulse streaming mechanism.
      // Removing this prevents pulse overcounting bug where non-log pulses were accumulated.

    } catch (Exception e) {
      android.util.Log.e(TAG, "Error processing hardware pulse packet: " + e.getMessage(), e);
      emitPacketProcessingError(deviceName, "HARDWARE_PULSE_ERROR", packet);
    }
  }

  /**
   * Industry standard: parse individual hardware pulse from packet data
   * Optimized byte-level parsing for high-frequency sensor data
   */
  private WritableMap parseHardwarePulse(byte[] packet, int offset) {
    try {
      WritableMap pulse = Arguments.createMap();

      // Industry standard: parse 32-bit timestamp (milliseconds)
      long timestamp = convertFourBytesToNumber(packet, offset);
      pulse.putDouble("timestamp", timestamp);

      // Industry standard: parse 32-bit pulse number
      long pulseNumber = convertFourBytesToNumber(packet, offset + 4);
      pulse.putDouble("pulseNumber", pulseNumber);

      // Industry standard: parse 16-bit burst and train numbers
      int burstNumber = convertTwoBytesToNumber(packet, offset + 8);
      int trainNumber = convertTwoBytesToNumber(packet, offset + 10);
      pulse.putInt("burstNumber", burstNumber);
      pulse.putInt("trainNumber", trainNumber);

      // Industry standard: parse MSO (16-bit, scaled by 10)
      int msoRaw = convertTwoBytesToNumber(packet, offset + 12);
      pulse.putDouble("mso", msoRaw / 10.0);

      // Industry standard: parse temperature sensors (4 x 16-bit, Celsius conversion)
      // FIXED: MSB-first byte order per AMPA documentation 2217[MSB] 2218[LSB]
      double temp1 = convertToCelsius(packet[offset + 14], packet[offset + 15]);
      double temp2 = convertToCelsius(packet[offset + 16], packet[offset + 17]);
      double temp3 = convertToCelsius(packet[offset + 18], packet[offset + 19]);
      double temp4 = convertToCelsius(packet[offset + 20], packet[offset + 21]);
      pulse.putDouble("temperature1", temp1);
      pulse.putDouble("temperature2", temp2);
      pulse.putDouble("temperature3", temp3);
      pulse.putDouble("temperature", temp4);

      // Industry standard: parse gyroscope data (3 x 16-bit, offset-corrected)
      int gyroX = convertTwoBytesToNumber(packet, offset + 22) - 360;
      int gyroY = convertTwoBytesToNumber(packet, offset + 24) - 360;
      int gyroZ = convertTwoBytesToNumber(packet, offset + 26) - 360;
      pulse.putInt("gyroX", gyroX);
      pulse.putInt("gyroY", gyroY);
      pulse.putInt("gyroZ", gyroZ);

      // Industry standard: parse accelerometer data (3 x 16-bit, offset-corrected)
      int accX = convertTwoBytesToNumber(packet, offset + 28) - 32000;
      int accY = convertTwoBytesToNumber(packet, offset + 30) - 32000;
      int accZ = convertTwoBytesToNumber(packet, offset + 32) - 32000;
      pulse.putInt("accX", accX);
      pulse.putInt("accY", accY);
      pulse.putInt("accZ", accZ);

      return pulse;

    } catch (Exception e) {
      android.util.Log.e(TAG, "Error parsing hardware pulse at offset " + offset + ": " + e.getMessage());
      return null;
    }
  }

  /**
   * Process sequence response packets (location = 2103)
   * Industry standard: command acknowledgment processing
   */
  private void processSequenceResponsePacket(String deviceName, byte[] packet) {
    try {
      android.util.Log.d(TAG, "Processing sequence response packet (" + packet.length + " bytes)");

      // Emit sequence response event
      WritableMap responseParams = Arguments.createMap();
      responseParams.putString("deviceName", deviceName);
      responseParams.putString("eventType", "SEQUENCE_RESPONSE");
      responseParams.putInt("packetLength", packet.length);
      eventEmit("onNativeSequenceResponse", responseParams);

      android.util.Log.d(TAG, "Sequence response processing completed for: " + deviceName);

    } catch (Exception e) {
      android.util.Log.e(TAG, "Error processing sequence response packet: " + e.getMessage(), e);
      emitPacketProcessingError(deviceName, "SEQUENCE_RESPONSE_ERROR", packet);
    }
  }

  /**
   * Process write acknowledgment packets (header 170)
   * These are command acknowledgments from the device, equivalent to JavaScript handleWriteCommand
   */
  private void processWriteAcknowledgment(String deviceName, byte[] packet) {
    try {
      android.util.Log.d(TAG, "Processing write acknowledgment (" + packet.length + " bytes)");

      // Extract command signature from bytes 1-4 for timeout clearing (matches JavaScript logic)
      if (packet.length >= 5) {
        // Create command signature array for timeout clearing
        byte[] commandSignature = new byte[5];
        System.arraycopy(packet, 0, commandSignature, 0, 5);

        // Clear command timeout using the acknowledgment signature
        // This matches the JavaScript handleWriteCommand(data) logic
        clearCommandTimeoutNative(deviceName, commandSignature);

        android.util.Log.d(TAG, "Write acknowledgment processed - Command signature: " +
                          java.util.Arrays.toString(commandSignature) +
                          ", Attempting to clear timeout for key: " + java.util.Arrays.toString(commandSignature));
      }

      // Write acknowledgment processed natively - no JavaScript event needed

      android.util.Log.d(TAG, "Write acknowledgment processing completed for: " + deviceName);

    } catch (Exception e) {
      android.util.Log.e(TAG, "Error processing write acknowledgment: " + e.getMessage(), e);
      emitPacketProcessingError(deviceName, "WRITE_ACKNOWLEDGMENT_ERROR", packet);
    }
  }

  /**
   * Clear command timeout using native acknowledgment
   * Equivalent to JavaScript SerialQueue.clearCommandTimeout()
   */
  private void clearCommandTimeoutNative(String deviceName, byte[] commandSignature) {
    try {
      // Generate command key using the same logic as CommandItem
      String key = new CommandItem(deviceName, commandSignature, 0, "", 0).commandKey;

      // Clear the retry timeout using existing mechanism
      ScheduledFuture<?> timeout = retryTimeouts.remove(key);
      if (timeout != null) {
        timeout.cancel(false);
        android.util.Log.d(TAG, "✅ Native command timeout cleared successfully: " + key);
      } else {
        android.util.Log.w(TAG, "❌ No timeout found for command key: " + key +
                          " (Active timeouts: " + retryTimeouts.size() + ")");
      }

    } catch (Exception e) {
      android.util.Log.e(TAG, "Error clearing command timeout: " + e.getMessage(), e);
    }
  }

  /**
   * Industry standard: emit packet processing error events
   * Provides structured error reporting for debugging and monitoring
   */
  private void emitPacketProcessingError(String deviceName, String errorType, byte[] packet) {
    try {
      WritableMap errorParams = Arguments.createMap();
      errorParams.putString("deviceName", deviceName);
      errorParams.putString("eventType", "PROCESSING_ERROR");
      errorParams.putString("errorType", errorType);
      errorParams.putInt("packetLength", packet != null ? packet.length : 0);

      // Include packet header for debugging (first 5 bytes)
      if (packet != null && packet.length >= 5) {
        WritableArray header = Arguments.createArray();
        for (int i = 0; i < Math.min(5, packet.length); i++) {
          header.pushInt(packet[i] & 0xFF);
        }
        errorParams.putArray("packetHeader", header);
      }

      eventEmit("onNativeProcessingError", errorParams);

    } catch (Exception e) {
      android.util.Log.e(TAG, "Error emitting packet processing error: " + e.getMessage());
    }
  }

  /**
   * Industry standard: convert four bytes to number (32-bit value parsing)
   * Used for timestamp and pulse number extraction
   */
  private long convertFourBytesToNumber(byte[] data, int offset) {
    if (data.length < offset + 4) {
      throw new IllegalArgumentException("Insufficient data for four-byte conversion");
    }
    return ((long)(data[offset] & 0xFF) << 24) |
           ((long)(data[offset + 1] & 0xFF) << 16) |
           ((long)(data[offset + 2] & 0xFF) << 8) |
           (long)(data[offset + 3] & 0xFF);
  }

  /**
   * Industry standard: convert two bytes to number (16-bit value parsing)
   * Used for sensor data extraction
   */
  private int convertTwoBytesToNumber(byte[] data, int offset) {
    if (data.length < offset + 2) {
      throw new IllegalArgumentException("Insufficient data for two-byte conversion");
    }
    return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
  }


  /**
   * Native data structures for device status parsing
   */
  public static class DeviceStatusData {
    // Bridge and system status
    public final boolean bridgeBoxPower;
    public final boolean magVentureEnabled;
    public final boolean pulseGeneratorOn;
    public final boolean coilFanRunning;
    public final boolean cameraEnabled; // Bit 4: Camera power
    public final boolean triggerPressed;

    // Power and voltage
    public final int mso;
    public final int voltage;
    public final double calculatedCapacitorVoltage;
    public final double chargeVoltage;

    // Temperature readings (4 sensors)
    public final double temperature1;
    public final double temperature2;
    public final double temperature3;
    public final double calculatedTemperature;

    // Motion and positioning
    public final int gyroX;
    public final int gyroY;
    public final int gyroZ;
    public final int accX;
    public final int accY;
    public final int accZ;

    // Status flags and configuration
    public final int currentMso;
    public final int currentDirection;
    public final int currentPolarityMode;
    public final int currentFrequency;
    public final int currentPulses;
    public final int currentDelay;
    public final int currentBurstMode;
    public final int currentCoilOutputA;
    public final int currentCoilOutputB;
    public final int currentInterStimInterval;
    public final int currentItiRandomizationPercentage;

    // Control and mode settings
    public final boolean isTriggerModeOn;
    public final boolean isTimed;
    public final boolean isRecordingSession;
    public final boolean isPlayingSession;
    public final int magneticFieldStrength;
    public final int playingTreatmentIndex;
    public final double outputVoltageA;
    public final double outputVoltageB;

    // Error states
    public final boolean capOvervoltage;
    public final boolean coilDisconnected;
    public final boolean coilNormalTemperature;
    public final boolean pgPulseDeliveringError;
    public final boolean usbConnected;
    public final boolean scrOverheated;
    public final boolean usbHubReset;
    public final boolean pgDisabledComReset;
    public final boolean pgFaultState;
    public final boolean psuOvervoltage;
    public final boolean coilOvercurrent;
    public final boolean bleederResistor;
    public final boolean hvOverheat;
    public final boolean lowCurrent;

    // Additional fields for complete SerialUtils.ts parity
    public final boolean manualCoilSwitchEnabled;
    public final int treatmentStatus;
    public final int pulsesInBurst;
    public final int burstsInTrain;
    public final int trainsInSequence;
    public final int lastPulseIndex;
    public final int pgType;
    public final String pgSerialNumber;
    public final long timestamp;
    public final int rotationX;
    public final int rotationY;
    public final int rotationZ;
    public final String coilType;
    public final String coilSerialNumber;
    // RevG additions
    public final int resistorTemperature; // °C (Integer.MIN_VALUE if buffer too short)
    public final int fanSpeed;            // actual fan %, 35-100 (-1 if buffer too short)
    public final int firmwareMajor;       // doc byte 4 -> buffer[9]
    public final int firmwareMinor;       // doc byte 5 -> buffer[10]
    public final boolean trainBackSupported; // fw at/beyond TRAIN_BACK_MIN_FW_MAJOR.MINOR

    public DeviceStatusData(byte[] buffer, String deviceName, java.util.concurrent.ConcurrentHashMap<String, Double> tempCache) {
      // Parse bridge and system status from buffer positions (buffer[5] = byte 0 in hardware spec)
      this.bridgeBoxPower = (buffer[5] & 0x01) != 0;      // Bit 0: Power up bit
      this.magVentureEnabled = (buffer[5] & 0x02) != 0;   // Bit 1: PG Enabled
      this.pulseGeneratorOn = (buffer[5] & 0x04) != 0;    // Bit 2: MagVenture PG Detection
      this.coilFanRunning = (buffer[5] & 0x08) != 0;      // Bit 3: Coil type control
      this.cameraEnabled = (buffer[5] & 0x10) != 0;       // Bit 4: Camera power
      this.triggerPressed = (buffer[5] & 0x20) != 0;      // Bit 5: USB_HUB was reset

      // Parse power and voltage (MSO from bytes 13-14 to match SerialUtils.ts exactly)
      this.mso = convertTwoBytes(buffer[13], buffer[14]) / 10;
      this.voltage = convertTwoBytes(buffer[9], buffer[8]);
      this.calculatedCapacitorVoltage = this.voltage * 10.0;
      this.chargeVoltage = convertTwoBytes(buffer[39], buffer[38]) / 10.0;

      // Parse temperature readings according to AMPA documentation (+5 offset for read command header)
      this.temperature1 = convertToCelsius(buffer[34], buffer[35]);        // Doc bytes 29[MSB], 30[LSB] → buffer[34], [35]
      this.temperature2 = convertToCelsius(buffer[36], buffer[37]);        // Doc bytes 31[MSB], 32[LSB] → buffer[36], [37]
      this.temperature3 = convertToCelsius(buffer[38], buffer[39]);        // Doc bytes 33[MSB], 34[LSB] → buffer[38], [39]

      // Apply smoothing to calculatedTemperature to reduce noise and fluctuations
      double rawCalculatedTemp = convertToCelsius(buffer[40], buffer[41]); // Doc bytes 35[MSB], 36[LSB] → buffer[40], [41]
      this.calculatedTemperature = RNSerialportModule.smoothTemperature(tempCache, deviceName, rawCalculatedTemp);

      // Motion data removed - conflicts with SerialUtils.ts field positions
      // SerialUtils.ts uses rotationX/Y/Z from bytes 46-51, not gyro/acc data
      this.gyroX = 0;
      this.gyroY = 0;
      this.gyroZ = 0;
      this.accX = 0;
      this.accY = 0;
      this.accZ = 0;

      // Firmware revision (doc bytes 4[major], 5[minor] -> buffer[9], buffer[10])
      this.firmwareMajor = buffer[9] & 0xFF;
      this.firmwareMinor = buffer[10] & 0xFF;
      this.trainBackSupported = (firmwareMajor > TRAIN_BACK_MIN_FW_MAJOR) ||
          (firmwareMajor == TRAIN_BACK_MIN_FW_MAJOR && firmwareMinor >= TRAIN_BACK_MIN_FW_MINOR);

      // Parse status flags and configuration
      this.currentMso = buffer[32] & 0xFF;
      this.currentDirection = buffer[33] & 0xFF;
      this.currentPolarityMode = buffer[34] & 0xFF;
      this.currentFrequency = convertTwoBytes(buffer[36], buffer[35]);
      this.currentPulses = convertTwoBytes(buffer[42], buffer[43]);
      this.currentDelay = convertTwoBytes(buffer[45], buffer[44]);
      this.currentBurstMode = buffer[46] & 0xFF;
      this.currentCoilOutputA = buffer[47] & 0xFF;
      this.currentCoilOutputB = buffer[48] & 0xFF;
      this.currentInterStimInterval = convertTwoBytes(buffer[50], buffer[49]);
      this.currentItiRandomizationPercentage = buffer[51] & 0xFF;

      // Parse control and mode settings
      this.isTriggerModeOn = (buffer[52] & 0x01) != 0;
      this.isTimed = (buffer[52] & 0x04) != 0;
      this.isRecordingSession = (buffer[52] & 0x08) != 0;
      this.isPlayingSession = (buffer[52] & 0x10) != 0;
      this.magneticFieldStrength = convertTwoBytes(buffer[54], buffer[53]);
      // Safely parse playing treatment index with bounds checking
      if (buffer.length > 56) {
        this.playingTreatmentIndex = convertTwoBytes(buffer[56], buffer[55]);
      } else {
        this.playingTreatmentIndex = 0;
      }
      // Safely parse voltage data with bounds checking
      if (buffer.length > 58) {
        this.outputVoltageA = convertTwoBytes(buffer[58], buffer[57]) / 10.0;
      } else {
        this.outputVoltageA = 0.0;
      }

      if (buffer.length > 60) {
        this.outputVoltageB = convertTwoBytes(buffer[60], buffer[59]) / 10.0;
      } else {
        this.outputVoltageB = 0.0;
      }

      // Parse error states from buffer[7] to match SerialUtils.ts
      this.pgPulseDeliveringError = (buffer[7] & 0x01) != 0; // errorDetected
      this.coilDisconnected = (buffer[7] & 0x02) != 0;
      this.coilNormalTemperature = (buffer[7] & 0x04) != 0;
      this.usbConnected = (buffer[7] & 0x08) != 0;
      this.scrOverheated = (buffer[7] & 0x10) != 0;
      this.capOvervoltage = (buffer[7] & 0x20) != 0;
      this.psuOvervoltage = (buffer[7] & 0x40) != 0;
      this.coilOvercurrent = (buffer[7] & 0x80) != 0;

      // Parse additional buffer[8] flags (Error LSB - byte 3)
      this.hvOverheat = (buffer[8] & 0x01) != 0;       // Bit 0: HV Board overheat
      this.bleederResistor = (buffer[8] & 0x02) != 0;  // Bit 1: Bleeder overheat
      this.lowCurrent = (buffer[8] & 0x04) != 0;       // Bit 2: Low current
      // Parse buffer[5] flags to match SerialUtils.ts
      this.usbHubReset = (buffer[5] & 0x20) != 0;
      this.pgDisabledComReset = (buffer[5] & 0x40) != 0;
      this.pgFaultState = (buffer[5] & 0x80) != 0;

      // Error states already parsed above from correct buffer positions

      // Parse additional fields for complete SerialUtils.ts parity
      this.manualCoilSwitchEnabled = (buffer[5] & 0x08) != 0;
      this.treatmentStatus = buffer[15] & 0xFF;
      this.pulsesInBurst = convertTwoBytes(buffer[16], buffer[17]);
      this.burstsInTrain = convertTwoBytes(buffer[18], buffer[19]);
      this.trainsInSequence = convertTwoBytes(buffer[20], buffer[21]);
      this.lastPulseIndex = convertTwoBytes(buffer[22], buffer[23]);
      this.pgType = buffer[24] & 0xFF;
      this.pgSerialNumber = String.format("%d%d%d",
        buffer[25] & 0xFF, buffer[26] & 0xFF, buffer[27] & 0xFF);
      this.timestamp = convertFourBytes(buffer[42], buffer[43], buffer[44], buffer[45]);
      this.rotationX = convertTwoBytes(buffer[46], buffer[47]);
      this.rotationY = convertTwoBytes(buffer[48], buffer[49]);
      this.rotationZ = convertTwoBytes(buffer[50], buffer[51]);
      this.coilType = String.valueOf((char) (buffer[52] & 0xFF));
      this.coilSerialNumber = String.format("%d%d%d",
        buffer[53] & 0xFF, buffer[54] & 0xFF, buffer[55] & 0xFF);
      // RevG: HV resistor temperature (doc byte 51 -> buffer[56], °C offset by 80)
      if (buffer.length > 56) {
        this.resistorTemperature = (buffer[56] & 0xFF) - 80;
      } else {
        this.resistorTemperature = Integer.MIN_VALUE;
      }
      // RevG: actual fan speed (doc byte 52 -> buffer[57], percent 35-100)
      if (buffer.length > 57) {
        this.fanSpeed = buffer[57] & 0xFF;
      } else {
        this.fanSpeed = -1;
      }
    }

    /**
     * Convert to WritableMap for React Native
     */
    public WritableMap toWritableMap() {
      WritableMap map = Arguments.createMap();

      // Bridge and system status
      map.putBoolean("bridgeBoxPower", bridgeBoxPower);
      map.putBoolean("magVentureEnabled", magVentureEnabled);
      map.putBoolean("pulseGeneratorOn", pulseGeneratorOn);
      map.putBoolean("coilFanRunning", coilFanRunning);
      map.putBoolean("cameraEnabled", cameraEnabled);  // Bit 4: Camera power from hardware
      map.putBoolean("triggerPressed", triggerPressed);

      // Power and voltage
      map.putInt("mso", mso);
      map.putInt("voltage", voltage);
      map.putDouble("calculatedCapacitorVoltage", calculatedCapacitorVoltage);
      map.putDouble("chargeVoltage", chargeVoltage);

      // Temperature readings
      map.putDouble("temperature1", temperature1);
      map.putDouble("temperature2", temperature2);
      map.putDouble("temperature3", temperature3);
      map.putDouble("calculatedTemperature", calculatedTemperature);
      // RevG additions (omit the key when the buffer was too short)
      if (resistorTemperature != Integer.MIN_VALUE) {
        map.putInt("resistorTemperature", resistorTemperature);
      }
      if (fanSpeed >= 0) {
        map.putInt("fanSpeed", fanSpeed);
      }
      map.putBoolean("trainBackSupported", trainBackSupported);

      // Motion and positioning
      map.putInt("gyroX", gyroX);
      map.putInt("gyroY", gyroY);
      map.putInt("gyroZ", gyroZ);
      map.putInt("accX", accX);
      map.putInt("accY", accY);
      map.putInt("accZ", accZ);

      // Status flags and configuration
      map.putInt("currentMso", currentMso);
      map.putInt("currentDirection", currentDirection);
      map.putInt("currentPolarityMode", currentPolarityMode);
      map.putInt("currentFrequency", currentFrequency);
      map.putInt("currentPulses", currentPulses);
      map.putInt("currentDelay", currentDelay);
      map.putInt("currentBurstMode", currentBurstMode);
      map.putInt("currentCoilOutputA", currentCoilOutputA);
      map.putInt("currentCoilOutputB", currentCoilOutputB);
      map.putInt("currentInterStimInterval", currentInterStimInterval);
      map.putInt("currentItiRandomizationPercentage", currentItiRandomizationPercentage);

      // Control and mode settings
      map.putBoolean("isTriggerModeOn", isTriggerModeOn);
      map.putBoolean("isTimed", isTimed);
      map.putBoolean("isRecordingSession", isRecordingSession);
      map.putBoolean("isPlayingSession", isPlayingSession);
      map.putInt("magneticFieldStrength", magneticFieldStrength);
      map.putInt("playingTreatmentIndex", playingTreatmentIndex);
      map.putDouble("outputVoltageA", outputVoltageA);
      map.putDouble("outputVoltageB", outputVoltageB);

      // Error states
      map.putBoolean("capOvervoltage", capOvervoltage);
      map.putBoolean("coilDisconnected", coilDisconnected);
      map.putBoolean("coilNormalTemperature", coilNormalTemperature);
      map.putBoolean("pgPulseDeliveringError", pgPulseDeliveringError);
      map.putBoolean("usbConnected", usbConnected);
      map.putBoolean("scrOverheated", scrOverheated);
      map.putBoolean("usbHubReset", usbHubReset);
      map.putBoolean("pgDisabledComReset", pgDisabledComReset);
      map.putBoolean("pgFaultState", pgFaultState);
      map.putBoolean("psuOvervoltage", psuOvervoltage);
      map.putBoolean("coilOvercurrent", coilOvercurrent);
      map.putBoolean("bleederResistor", bleederResistor);
      map.putBoolean("hvOverheat", hvOverheat);
      map.putBoolean("lowCurrent", lowCurrent);

      // Additional fields for complete SerialUtils.ts parity
      map.putBoolean("manualCoilSwitchEnabled", manualCoilSwitchEnabled);
      map.putInt("treatmentStatus", treatmentStatus);
      map.putInt("pulsesInBurst", pulsesInBurst);
      map.putInt("burstsInTrain", burstsInTrain);
      map.putInt("trainsInSequence", trainsInSequence);
      map.putInt("lastPulseIndex", lastPulseIndex);
      map.putInt("pgType", pgType);
      map.putString("pgSerialNumber", pgSerialNumber);
      map.putInt("rotationX", rotationX);
      map.putInt("rotationY", rotationY);
      map.putInt("rotationZ", rotationZ);
      map.putString("coilType", coilType);
      map.putString("coilSerialNumber", coilSerialNumber);

      return map;
    }
  }

  /**
   * Native data structure for hardware pulse data
   */
  public static class HardwarePulse {
    public final long timestamp;
    public final long pulseNumber;
    public final int burstNumber;
    public final int trainNumber;
    public final double mso;
    public final double temperature1;
    public final double temperature2;
    public final double temperature3;
    public final double temperature;
    public final int gyroX;
    public final int gyroY;
    public final int gyroZ;
    public final int accX;
    public final int accY;
    public final int accZ;

    public HardwarePulse(byte[] data) {
      this.timestamp = convertFourBytes(data[0], data[1], data[2], data[3]);
      this.pulseNumber = convertFourBytes(data[4], data[5], data[6], data[7]);
      this.burstNumber = convertTwoBytes(data[8], data[9]);
      this.trainNumber = convertTwoBytes(data[10], data[11]);
      this.mso = convertTwoBytes(data[12], data[13]) / 10.0;
      this.temperature1 = convertToCelsius(data[14], data[15]);
      this.temperature2 = convertToCelsius(data[16], data[17]);
      this.temperature3 = convertToCelsius(data[18], data[19]);
      this.temperature = convertToCelsius(data[20], data[21]);
      this.gyroX = convertTwoBytes(data[22], data[23]) - 360;
      this.gyroY = convertTwoBytes(data[24], data[25]) - 360;
      this.gyroZ = convertTwoBytes(data[26], data[27]) - 360;
      this.accX = convertTwoBytes(data[28], data[29]) - 32000;
      this.accY = convertTwoBytes(data[30], data[31]) - 32000;
      this.accZ = convertTwoBytes(data[32], data[33]) - 32000;
    }

    public WritableMap toWritableMap() {
      WritableMap map = Arguments.createMap();
      map.putDouble("timestamp", timestamp);
      map.putDouble("pulseNumber", pulseNumber);
      map.putInt("burstNumber", burstNumber);
      map.putInt("trainNumber", trainNumber);
      map.putDouble("mso", mso);
      map.putDouble("temperature1", temperature1);
      map.putDouble("temperature2", temperature2);
      map.putDouble("temperature3", temperature3);
      map.putDouble("temperature", temperature);
      map.putInt("gyroX", gyroX);
      map.putInt("gyroY", gyroY);
      map.putInt("gyroZ", gyroZ);
      map.putInt("accX", accX);
      map.putInt("accY", accY);
      map.putInt("accZ", accZ);
      return map;
    }
  }

  /**
   * Sliding window validator for device status timestamps.
   * Maintains per-device history of recent valid timestamps and rejects outliers
   * (garbled bytes, reconnection artifacts) by comparing against the window.
   *
   * On reject: caller omits timestamp from JS event, so currentDuration is not corrupted.
   * On treatmentStatus change: window is cleared to allow legitimate timer resets.
   */
  public static class TimestampValidator {
    private static final int  WINDOW_SIZE         = 10;
    private static final long MAX_DELTA_MS        = 10_000L;
    // Gap-recovery: after MAX_DELTA_MS rejection, accept once we see N consecutive
    // rejected timestamps that are themselves coherent (monotonic, ~heartbeat-spaced).
    // A real connection gap produces a ticking stream on resume; garbage produces
    // isolated spikes. 3 rejections at the 250ms heartbeat = ~750ms recovery latency.
    private static final int  ACCEPT_AFTER_N      = 3;
    private static final long REJECT_COHERENCE_MS = 2_000L;

    private final long[] window = new long[WINDOW_SIZE];
    private int  count = 0;
    private int  lastTreatmentStatus = -1;

    private long lastRejected = -1;
    private int  rejectStreak = 0;

    public synchronized long validate(long rawTimestamp, int treatmentStatus) {
      // Treatment status change is the only legitimate timer reset — clear window
      if (lastTreatmentStatus >= 0 && lastTreatmentStatus != treatmentStatus) {
        clear();
        Log.i(TAG, "TIMESTAMP_VALIDATOR: Window cleared (treatmentStatus " +
              lastTreatmentStatus + " -> " + treatmentStatus + ")");
      }
      lastTreatmentStatus = treatmentStatus;

      if (count == 0) {
        if (rawTimestamp < 0) {
          Log.w(TAG, "TIMESTAMP_VALIDATOR: Negative timestamp rejected: " + rawTimestamp);
          return -1;
        }
        window[0] = rawTimestamp;
        count = 1;
        return rawTimestamp;
      }

      long lastValid = window[(count - 1) % WINDOW_SIZE];

      if (rawTimestamp < lastValid) {
        rejectStreak = 0;
        lastRejected = -1;
        Log.w(TAG, "TIMESTAMP_VALIDATOR: Backward jump rejected (last=" + lastValid +
              ", new=" + rawTimestamp + "), returning lastValid");
        return lastValid;
      }

      long delta = rawTimestamp - lastValid;
      if (delta > MAX_DELTA_MS) {
        boolean coherent = lastRejected > 0
            && rawTimestamp >= lastRejected
            && (rawTimestamp - lastRejected) <= REJECT_COHERENCE_MS;

        rejectStreak = coherent ? rejectStreak + 1 : 1;
        lastRejected = rawTimestamp;

        if (rejectStreak >= ACCEPT_AFTER_N) {
          Log.w(TAG, "TIMESTAMP_VALIDATOR: Gap recovery — accepting after " +
                rejectStreak + " coherent rejections (last=" + lastValid +
                ", new=" + rawTimestamp + ", delta=" + delta + "ms)");
          count = 1;
          window[0] = rawTimestamp;
          rejectStreak = 0;
          lastRejected = -1;
          return rawTimestamp;
        }

        Log.w(TAG, "TIMESTAMP_VALIDATOR: Large delta rejected (last=" + lastValid +
              ", new=" + rawTimestamp + ", delta=" + delta + "ms, streak=" +
              rejectStreak + "), returning lastValid");
        return lastValid;
      }

      rejectStreak = 0;
      lastRejected = -1;
      count++;
      window[(count - 1) % WINDOW_SIZE] = rawTimestamp;
      return rawTimestamp;
    }

    public synchronized void clear() {
      count = 0;
      lastTreatmentStatus = -1;
      lastRejected = -1;
      rejectStreak = 0;
    }
  }

  /**
   * Native utility methods for data conversion and validation
   */
  private static int convertTwoBytes(byte high, byte low) {
    return ((high & 0xFF) << 8) | (low & 0xFF);
  }

  private static long convertFourBytes(byte byte1, byte byte2, byte byte3, byte byte4) {
    return ((byte1 & 0xFFL) << 24) | ((byte2 & 0xFFL) << 16) | ((byte3 & 0xFFL) << 8) | (byte4 & 0xFFL);
  }

  private static double convertToCelsius(byte highByte, byte lowByte) {
    int rawValue = convertTwoBytes(highByte, lowByte);
    if (rawValue == 0) return 0.0;
    // Official AMPA documentation temperature conversion formula
    // Temperature in X°C × 10, offset by 800 (e.g. 0°C ⬄ data=800d=320h)
    return (rawValue - 800) / 10.0;
  }

  /**
   * Apply exponential moving average (EMA) smoothing to temperature readings
   * Reduces noise and fluctuations in calculatedTemperature values
   *
   * @param cache Temperature cache map
   * @param deviceName Device identifier for temperature cache
   * @param rawTemp Raw temperature reading from device
   * @return Smoothed temperature value
   */
  private static double smoothTemperature(java.util.concurrent.ConcurrentHashMap<String, Double> cache,
                                          String deviceName,
                                          double rawTemp) {
    // Get previous smoothed value from cache
    Double previousTemp = cache.get(deviceName);

    // Reject physically impossible readings — corrupted bytes produce values like 779°C
    // which would poison the EMA cache. Return cached value if available, else 0.
    if (rawTemp < -20.0 || rawTemp > 100.0) {
      return previousTemp != null ? previousTemp : 0.0;
    }

    // First reading for this device - no smoothing needed
    if (previousTemp == null) {
      cache.put(deviceName, rawTemp);
      return rawTemp;
    }

    // Apply EMA: smoothed = (alpha × new) + ((1 - alpha) × previous)
    // Alpha = 0.3 means 30% of new value, 70% of previous value
    double smoothedTemp = (TEMPERATURE_SMOOTHING_FACTOR * rawTemp) +
                          ((1 - TEMPERATURE_SMOOTHING_FACTOR) * previousTemp);

    // Round to 1 decimal place to match original precision
    smoothedTemp = Math.round(smoothedTemp * 10.0) / 10.0;

    // Update cache with new smoothed value
    cache.put(deviceName, smoothedTemp);

    return smoothedTemp;
  }

  private static boolean validateDataPacket(byte[] data, int expectedLength) {
    if (data == null || data.length < expectedLength) {
      return false;
    }

    // Skip temperature range checks - accept all packets with valid length
    return true;
  }

  /**
   * Native enhanced read data processing with structured events
   */
  @ReactMethod
  public void processNativeReadData(String deviceName, ReadableArray rawData) {
    try {
      // Convert ReadableArray to byte array
      byte[] data = new byte[rawData.size()];
      for (int i = 0; i < rawData.size(); i++) {
        data[i] = (byte) rawData.getInt(i);
      }

      // Validate packet integrity - temporarily accept 56 bytes for debugging
      if (!validateDataPacket(data, 56)) {
        android.util.Log.w(TAG, "Invalid data packet received - insufficient length or disconnected coil");
        return;
      }

      // Log packet details for debugging
      if (data.length != 63) {
        android.util.Log.i(TAG, "Processing packet with " + data.length + " bytes (expected 63)");
      }

      // Parse device status using native data structure with temperature smoothing
      DeviceStatusData deviceStatus = new DeviceStatusData(data, deviceName, temperatureCache);

      // Run packet integrity validation
      PacketIntegrityValidator.Result integrityResult = packetIntegrityValidator.validate(data);

      // If integrity failed: emit event for JS logging, then bail out
      if (integrityResult.failed) {
        WritableMap statusParams = Arguments.createMap();
        statusParams.putString("deviceName", deviceName);
        statusParams.putString("eventType", "DEVICE_STATUS");
        WritableMap dataMap = deviceStatus.toWritableMap();
        dataMap.putInt("integrityScore", integrityResult.score);
        dataMap.putBoolean("integrityFailed", integrityResult.failed);
        dataMap.putString("integrityViolations", integrityResult.violations);
        statusParams.putMap("data", dataMap);
        eventEmit("onNativeDeviceStatus", statusParams);
        return;
      }

      // Emit structured device status event
      WritableMap statusParams = Arguments.createMap();
      statusParams.putString("deviceName", deviceName);
      statusParams.putString("eventType", "DEVICE_STATUS");
      WritableMap dataMap = deviceStatus.toWritableMap();

      // Validate timestamp against sliding window before emitting to JS
      long validatedTimestamp = timestampValidator.validate(deviceStatus.timestamp, deviceStatus.treatmentStatus);
      if (validatedTimestamp >= 0) {
        dataMap.putDouble("timestamp", validatedTimestamp);
      }

      // Add integrity check results to the existing data map
      dataMap.putInt("integrityScore", integrityResult.score);
      dataMap.putBoolean("integrityFailed", integrityResult.failed);
      dataMap.putString("integrityViolations", integrityResult.violations);

      statusParams.putMap("data", dataMap);
      eventEmit("onNativeDeviceStatus", statusParams);

      // Auto-ramp: check timeline keyframes; emit if a manual override was detected
      boolean autoRampOverride = autoRampEngine.checkAndApply(deviceName, deviceStatus.timestamp, deviceStatus.treatmentStatus, deviceStatus.mso, deviceStatus.lastPulseIndex, deviceStatus.trainsInSequence);
      if (autoRampOverride) {
        WritableMap overrideParams = Arguments.createMap();
        overrideParams.putString("deviceName", deviceName);
        eventEmit("onAutoRampManualOverride", overrideParams);
      }

      // Extract and emit power status if changed
      WritableMap powerParams = Arguments.createMap();
      powerParams.putString("deviceName", deviceName);
      powerParams.putString("eventType", "POWER_STATUS");
      powerParams.putBoolean("pulseGeneratorOn", deviceStatus.pulseGeneratorOn);
      powerParams.putInt("voltage", deviceStatus.voltage);
      powerParams.putDouble("chargeVoltage", deviceStatus.chargeVoltage);
      eventEmit("onNativePowerStatus", powerParams);

      // Play status, MSO status, and error states are now included in onNativeDeviceStatus and processed in JS
      // This eliminates duplicate events and ensures single source of truth
      // All processing occurs in NativeReadBridge.handleDeviceStatus()

      android.util.Log.d(TAG, "Native read data processing completed for device: " + deviceName);

    } catch (Exception e) {
      android.util.Log.e(TAG, "Error processing native read data: " + e.getMessage(), e);

      // Emit error event
      WritableMap errorParams = Arguments.createMap();
      errorParams.putString("deviceName", deviceName);
      errorParams.putString("eventType", "PROCESSING_ERROR");
      errorParams.putString("error", e.getMessage());
      eventEmit("onNativeProcessingError", errorParams);
    }
  }

  ///////////////////////////////////////////////Native Log Export API /////////////////////////////////////////////////////////
  ///////////////////////////////////////////////Native Log Export API /////////////////////////////////////////////////////////


  ///////////////////////////////////////////////Native Log Collection Manager /////////////////////////////////////////////////////////
  ///////////////////////////////////////////////Native Log Collection Manager /////////////////////////////////////////////////////////

  // Constants for log operations
  private static final int COMMAND_READ = 0x66;
  private static final int COMMAND_WRITE = 0xaa;
  private static final int PULSE_SIZE = 34;
  private static final int PULSE_START_ADDRESS = 2201;
  private static final int LOG_RESET_ADDRESS = 2121;
  private static final int MAX_PULSES_PER_FETCH = 7;

  // Native log collection state
  private final Map<String, Integer> deviceLogIndex = new ConcurrentHashMap<>();
  private final Map<String, Integer> deviceTargetPulses = new ConcurrentHashMap<>();
  private final Map<String, Boolean> deviceLoggingActive = new ConcurrentHashMap<>();
  private volatile int lastProcessedTreatmentStatus = -1;
  private volatile int lastProcessedPulseIndex = -1;

  // Native-only acknowledgment system for duplicate prevention
  private final Map<String, Boolean> deviceWaitingForResponse = new ConcurrentHashMap<>();
  private final Map<String, Integer> deviceExpectedPulseCount = new ConcurrentHashMap<>();
  private final Map<String, Integer> deviceRetryCount = new ConcurrentHashMap<>();
  private final Map<String, Runnable> deviceTimeoutHandlers = new ConcurrentHashMap<>();
  // LOG_READ_TIMEOUT_MS moved to timing constants section at top of file (line 162)

  // Retry mechanism for missing batches
  private static final int MAX_RETRY_ATTEMPTS = 1; // Single retry attempt for missing batches

  // Pulse sequence tracking for duplicate detection (Set of pulse numbers seen per device)
  // Changed from Integer (highest) to Set<Integer> (exact pulses) to handle out-of-order delivery
  private final Map<String, Set<Integer>> deviceSeenPulseNumbers = new ConcurrentHashMap<>();

  // ACK-based completion tracking (batch acknowledgment protocol)
  private final Map<String, Integer> devicePulseBatchesSent = new ConcurrentHashMap<>();
  private final Map<String, Integer> devicePulseBatchesAcked = new ConcurrentHashMap<>();
  private final Map<String, Runnable> deviceCompletionTimeoutRunnables = new ConcurrentHashMap<>();

  // CRITICAL: Shared Handler for timeout management (must use same instance to post and remove callbacks)
  private final Handler timeoutHandler = new Handler(Looper.getMainLooper());

  // CRITICAL: Global log collection state for heartbeat management
  private volatile boolean isCollectingLogs = false;
  private volatile String logCollectionDevice = null;

  // Auto-resume state tracking for device reconnection during log collection
  private volatile boolean wasCollectingLogs = false;
  private volatile String lastCollectionDevice = null;
  private volatile int lastExpectedPulseIndex = 0;

  // Auto-resume validation constants
  private static final int VALIDATION_TIMEOUT_MS = 3000; // 3 seconds for validation
  private final Map<String, Runnable> deviceValidationTimeouts = new ConcurrentHashMap<>();

  /**
   * Helper method to convert number to three-byte array for memory addressing
   */
  private byte[] toThreeBytes(int number) {
    if (number < 0 || number > 16777215) {
      return new byte[]{0x00, 0x00, 0x00};
    }
    return new byte[]{
        (byte) ((number >> 16) & 0xFF), // MSB
        (byte) ((number >> 8) & 0xFF),  // Mid
        (byte) (number & 0xFF)          // LSB
    };
  }

  /**
   * Centralized cleanup for ALL log collection scenarios
   * Ensures heartbeat is ALWAYS resumed and state is cleaned
   *
   * @param deviceName Device to clean up
   * @param resumeHeartbeat Whether to resume heartbeat (false for errors that will retry)
   */
  private void cleanupLogCollectionState(String deviceName, boolean resumeHeartbeat) {
    cleanupLogCollectionState(deviceName, resumeHeartbeat, false);
  }

  /**
   * Centralized cleanup with option to clear pulse tracking
   *
   * @param deviceName Device to clean up
   * @param resumeHeartbeat Whether to resume heartbeat
   * @param clearPulseTracking Whether to clear deviceSeenPulseNumbers (only on treatment end/cancel)
   */
  private void cleanupLogCollectionState(String deviceName, boolean resumeHeartbeat, boolean clearPulseTracking) {
    try {
      // DIAGNOSTIC: Log stack trace to identify what triggered cleanup
      android.util.Log.w(TAG, "🧹 CLEANUP TRIGGERED for: " + deviceName +
                        " (resumeHeartbeat=" + resumeHeartbeat + ", clearPulseTracking=" + clearPulseTracking + ")" +
                        "\nStack trace:\n" + android.util.Log.getStackTraceString(new Exception()));

      // Clear all device-specific state
      deviceLoggingActive.put(deviceName, false);
      deviceLogIndex.remove(deviceName);
      deviceTargetPulses.remove(deviceName);
      deviceWaitingForResponse.remove(deviceName);
      deviceExpectedPulseCount.remove(deviceName);
      deviceRetryCount.remove(deviceName);

      // CRITICAL: Only clear pulse tracking when treatment is finished or manually cancelled
      // NOT on disconnect - we want to remember which pulses were already collected
      // This prevents duplicate pulses if log collection resumes after device reconnect
      if (clearPulseTracking) {
        Set<Integer> seenPulses = deviceSeenPulseNumbers.get(deviceName);
        if (seenPulses != null) {
          seenPulses.clear();
          deviceSeenPulseNumbers.remove(deviceName);
        }
        android.util.Log.d(TAG, "🧹 Cleared pulse sequence tracking set for device");
      } else {
        android.util.Log.d(TAG, "🧹 Preserving pulse sequence tracking for potential reconnect/resume");
      }

      // FIX: Reset treatment status tracking to allow next sequence to trigger log collection
      // When log collection is cancelled during sequence transition, we need to clear these
      // so the next sequence's 1→0 transition is properly detected
      lastProcessedTreatmentStatus = -1;
      lastProcessedPulseIndex = -1;
      deviceSuppressNextLogCollection.remove(deviceName);
      android.util.Log.d(TAG, "🧹 Reset treatment status tracking for next sequence");

      // Flush the per-device packet buffer, but do NOT remove it: this cleanup runs while the
      // device is still connected (e.g. log-collection completed), and removing the buffer would
      // drop its CRC framing flag. The next batch would recreate a fresh buffer defaulting to
      // CRC-off while V0.16+ firmware keeps sending CRC-trailered frames — a permanent framing
      // desync. clear() empties buffered bytes while preserving crcEnabled (same as the disconnect
      // path in stopConnection). The buffer is fully removed only on real disconnect.
      NativePacketBuffer buffer = devicePacketBuffers.get(deviceName);
      if (buffer != null) {
        buffer.clear();
        android.util.Log.d(TAG, "🧹 Cleared packet buffer for device: " + deviceName);
      }

      // Cancel any pending timeout handlers
      Runnable timeoutRunnable = deviceTimeoutHandlers.remove(deviceName);
      if (timeoutRunnable != null) {
        timeoutHandler.removeCallbacks(timeoutRunnable);
        android.util.Log.d(TAG, "🧹 Cancelled pending timeout handler");
      }

      // PHASE 1: Cancel validation timeout if present
      Runnable validationTimeout = deviceValidationTimeouts.remove(deviceName);
      if (validationTimeout != null) {
        timeoutHandler.removeCallbacks(validationTimeout);
        android.util.Log.d(TAG, "🧹 Cancelled validation timeout handler");
      }

      // PHASE 2: Cancel log collection completion timeout (inactivity/absolute timer)
      // CRITICAL: This prevents orphan timer from firing after user cancels log collection
      Runnable completionTimeout = deviceCompletionTimeoutRunnables.remove(deviceName);
      if (completionTimeout != null) {
        timeoutHandler.removeCallbacks(completionTimeout);
        android.util.Log.d(TAG, "🧹 Cancelled log collection completion timeout (inactivity/absolute)");
      }

      // Reset pulse tracking (thread-safe)
      lastPulseNumber.set(-1);

      // Clear global log collection state - heartbeat automatically switches back to full mode
      if (deviceName != null && deviceName.equals(logCollectionDevice)) {
        isCollectingLogs = false;
        logCollectionDevice = null;
        android.util.Log.i(TAG, "💓 Heartbeat switching back to full status mode");
      } else {
        android.util.Log.w(TAG, "⚠️ Different device was collecting logs");
      }

    } catch (Exception e) {
      android.util.Log.e(TAG, "❌ Error during cleanup: " + e.getMessage());
      // SAFETY: Force cleanup even on error - heartbeat continues running
      isCollectingLogs = false;
      logCollectionDevice = null;
      android.util.Log.i(TAG, "💓 Heartbeat will automatically switch back to full mode");
    }
  }

  /**
   * Auto-start log collection when treatment completes
   * Called from processHardwarePulsePacket when hardware indicates completion
   */
  private void autoStartLogCollection(String deviceName, int lastPulseIndex) {
    try {
      if (lastPulseIndex <= 0) {
        return;
      }

      // Check suppress flag (set by JS before discard/stop to prevent unwanted log collection)
      if (deviceSuppressNextLogCollection.getOrDefault(deviceName, false)) {
        android.util.Log.i(TAG, "⏭️ Suppressing auto-log collection for: " + deviceName + " (discard/cancel requested)");
        deviceSuppressNextLogCollection.put(deviceName, false);
        return;
      }

      // CRITICAL: Check if already collecting for THIS device
      if (deviceLoggingActive.getOrDefault(deviceName, false)) {
        android.util.Log.w(TAG, "⚠️ Log collection already active for: " + deviceName);
        return;
      }

      // CRITICAL: Check if collecting for ANOTHER device
      if (isCollectingLogs && deviceName != null && !deviceName.equals(logCollectionDevice)) {
        android.util.Log.w(TAG, "⚠️ Cannot start collection - another device active: " +
                          logCollectionDevice);
        return;
      }

      android.util.Log.i(TAG, "🔄 Starting log collection: " + deviceName +
                        " (" + lastPulseIndex + " pulses)");

      // Set collection flags
      isCollectingLogs = true;
      logCollectionDevice = deviceName;

      // Initialize logging state
      deviceLogIndex.put(deviceName, 0);
      deviceTargetPulses.put(deviceName, lastPulseIndex);
      deviceLoggingActive.put(deviceName, true);

      // CRITICAL FIX: Reset batch counters for new sequence
      // Without this, batch numbers continue from previous sequence causing incorrect completion logic
      devicePulseBatchesSent.remove(deviceName);
      devicePulseBatchesAcked.remove(deviceName);
      android.util.Log.d(TAG, "🔄 Reset batch counters for new log collection sequence");

      // CRITICAL: DO NOT clear the Set here - keep live treatment pulses to reject duplicates
      // Log collection reads from device MEMORY (which contains ALL pulses from treatment)
      // These memory pulses are the SAME ones we already received during live treatment
      // By keeping the Set, we reject these duplicate memory reads
      // The Set will be cleared in cleanupLogCollectionState() after log completes/cancels/errors
      // FIX #8: Use computeIfAbsent() to fix Set initialization race condition
      // RACE CONDITION: get() + null check + create + put() is NOT atomic across threads
      // Multiple threads can create separate Set instances, overwriting each other and losing data
      Set<Integer> seenPulses = deviceSeenPulseNumbers.computeIfAbsent(
          deviceName,
          k -> ConcurrentHashMap.newKeySet()
      );
      android.util.Log.d(TAG, "🔢 Log collection starting: Set has " + seenPulses.size() + " pulses from live treatment, expecting " + lastPulseIndex + " total");

      // Emit START status for UI progress modal
      WritableMap params = Arguments.createMap();
      params.putString("deviceName", deviceName);
      params.putString("status", "START");
      eventEmit("onNativeLogStatusChange", params);

      // Start reading logs automatically
      nativeReadLogData(deviceName);

    } catch (Exception e) {
      android.util.Log.e(TAG, "❌ Error starting log collection: " + e.getMessage());
      // CRITICAL: Clean up on error
      cleanupLogCollectionState(deviceName, true);
      emitLogError(deviceName, "Failed to start log collection");
    }
  }

  /**
   * Native log data reader with THROTTLED QUEUEING approach
   * Queues log commands with 200ms delays to give device time to respond
   * This matches the ~250ms timing that worked in JS version
   */
  private void nativeReadLogData(String deviceName) {
    try {
      int totalPulses = deviceTargetPulses.getOrDefault(deviceName, 0);

      android.util.Log.i(TAG, "🚀 THROTTLED QUEUE: Scheduling log commands with " + LOG_COMMAND_INTERVAL_MS + "ms delays for " + totalPulses + " pulses");

      // Mark as collecting (but NOT waiting for individual responses)
      deviceWaitingForResponse.put(deviceName, false);

      int currentIndex = 0;
      int commandIndex = 0;

      // Schedule log commands with delays to give device time to respond
      while (currentIndex < totalPulses) {
        int remainingPulses = totalPulses - currentIndex;
        int actualPulses = Math.min(MAX_PULSES_PER_FETCH, remainingPulses);

        // Calculate delay for this command (200ms per command)
        final int index = currentIndex;
        final int pulses = actualPulses;
        int delayMs = commandIndex * LOG_COMMAND_INTERVAL_MS;

        // Schedule command to be queued after delay
        timeoutHandler.postDelayed(() -> {
          sendLogReadCommand(deviceName, index, pulses, totalPulses);
        }, delayMs);

        currentIndex += actualPulses;
        commandIndex++;
      }

      // INACTIVITY-BASED TIMEOUT APPROACH:
      // - No initial timer set here - timer starts/resets on each ACK receipt
      // - Completion triggers 10 seconds after LAST ACK received (inactivity window)
      // - 2-minute absolute timeout as safety net in case NO ACKs ever received
      int totalCommands = (int) Math.ceil((double) totalPulses / MAX_PULSES_PER_FETCH);
      int allCommandsSentTime = totalCommands * LOG_COMMAND_INTERVAL_MS; // 200ms per command

      android.util.Log.i(TAG, "⏱️ Scheduled " + totalCommands + " commands (will send over ~" +
                        allCommandsSentTime + "ms)");
      android.util.Log.i(TAG, "⏱️ Inactivity-based completion: 10s after last ACK (resets on each ACK)");
      android.util.Log.i(TAG, "⏱️ Absolute safety timeout: " + (ABSOLUTE_TIMEOUT_MS/60000) + " minutes");

      // Schedule ABSOLUTE timeout (only triggers if JS never sends ANY ACKs - crash before first ACK)
      Runnable absoluteTimeoutRunnable = () -> {
        android.util.Log.w(TAG, "⏱️ ABSOLUTE TIMEOUT: 2 minutes reached with no completion, forcing finish");
        int sent = devicePulseBatchesSent.getOrDefault(deviceName, 0);
        int acked = devicePulseBatchesAcked.getOrDefault(deviceName, 0);
        android.util.Log.w(TAG, "⏱️ Final ACK status: " + acked + "/" + sent + " batches acknowledged");
        android.util.Log.w(TAG, "⚠️ This is a safety fallback - indicates severe JS processing failure");
        completeLogCollection(deviceName);
      };

      deviceCompletionTimeoutRunnables.put(deviceName, absoluteTimeoutRunnable);
      timeoutHandler.postDelayed(absoluteTimeoutRunnable, ABSOLUTE_TIMEOUT_MS);

    } catch (Exception e) {
      android.util.Log.e(TAG, "❌ ERROR: Exception in throttled queue log read: " + e.getMessage());
      emitLogError(deviceName, "Failed to start log collection");
      cleanupLogCollectionState(deviceName, true);
    }
  }

  /**
   * Helper: Send a single log read command (used by fire-and-forget)
   */
  private void sendLogReadCommand(String deviceName, int index, int pulses, int totalPulses) {
    try {
      // CRITICAL: Verify log collection is still active before sending command
      // This prevents scheduled commands from executing after cleanup/completion
      if (!deviceLoggingActive.getOrDefault(deviceName, false)) {
        android.util.Log.w(TAG, "⚠️ SKIP: Log command #" + (index/MAX_PULSES_PER_FETCH) +
                          " - collection no longer active for: " + deviceName);
        return;
      }

      Integer targetPulses = deviceTargetPulses.get(deviceName);
      if (targetPulses == null) {
        android.util.Log.w(TAG, "⚠️ SKIP: Log command #" + (index/MAX_PULSES_PER_FETCH) +
                          " - target pulses cleared for: " + deviceName);
        return;
      }

      // FIX #3: Safety check - should never happen due to scheduling loop validation
      // But if it does (due to concurrent modification or race condition), skip to prevent device errors
      if (index >= totalPulses) {
        android.util.Log.e(TAG, "❌ SAFETY: Batch index " + index +
                          " >= totalPulses " + totalPulses + " - SKIPPING (should not happen!)");
        return;
      }

      // FIX #3: Diagnostic - warn if pulse count looks wrong (scheduling loop should prevent this)
      if (index + pulses > totalPulses) {
        android.util.Log.w(TAG, "⚠️ DIAGNOSTIC: Batch at index " + index + " requests " + pulses +
                          " pulses but only " + (totalPulses - index) + " remain. " +
                          "Scheduling loop should have adjusted this. Proceeding with original request.");
        // NOTE: Do NOT adjust here - trust the scheduling loop's calculation
        // Adjusting here could cause double-adjustment and incorrect progress reporting
      }

      // Build read command
      byte[] command = new byte[5];
      command[0] = (byte) COMMAND_READ;

      int pulseLocation = PULSE_START_ADDRESS + index * PULSE_SIZE;
      byte[] location = toThreeBytes(pulseLocation);
      System.arraycopy(location, 0, command, 1, 3);
      command[4] = (byte) (pulses * PULSE_SIZE);

      android.util.Log.d(TAG, "📤 Fire: index=" + index + " pulses=" + pulses +
                        " (" + index + "/" + totalPulses + "), batch #" + (index/MAX_PULSES_PER_FETCH + 1));

      // PRIORITY QUEUE: Log commands get PRIORITY_FRONT (3) > heartbeats (1)
      // This ensures all log commands execute before any heartbeats in queue
      addToNativeQueue(deviceName, command, PRIORITY_FRONT, "log-read-" + index, 0);

      // FIX #5: Progress emission removed from here - now emitted on ACK receipt
      // This ensures progress reflects actual data received, not commands fired

    } catch (Exception e) {
      android.util.Log.e(TAG, "❌ ERROR: Failed to send log command at index " + index + ": " + e.getMessage());
    }
  }

  /**
   * Reset the inactivity timer - triggers 10 seconds after LAST ACK received
   * This implements the inactivity-based timeout approach where completion occurs
   * after a period of no ACK activity, rather than a fixed timeout from start.
   *
   * Called on every ACK receipt to reset the 10-second window.
   *
   * @param deviceName Device identifier
   */
  private void resetInactivityTimer(String deviceName) {
    // CRITICAL: Check if log collection is still active
    // Prevents scheduling new timers after cancellation (delayed commands may still trigger ACKs)
    if (!deviceLoggingActive.getOrDefault(deviceName, false)) {
      android.util.Log.d(TAG, "⏭️ SKIP timer reset - log collection no longer active");
      return;
    }

    // Cancel any existing inactivity timer
    Runnable existingTimer = deviceCompletionTimeoutRunnables.get(deviceName);
    if (existingTimer != null) {
      timeoutHandler.removeCallbacks(existingTimer);
      android.util.Log.d(TAG, "⏱️ Inactivity timer RESET (10s from now)");
    } else {
      android.util.Log.d(TAG, "⏱️ Inactivity timer STARTED (10s until completion)");
    }

    // Create new inactivity timer (10 seconds)
    Runnable inactivityTimer = () -> {
      android.util.Log.w(TAG, "⏱️ INACTIVITY TIMEOUT: No ACKs received for 10 seconds");

      // Diagnostic logging
      int sent = devicePulseBatchesSent.getOrDefault(deviceName, 0);
      int acked = devicePulseBatchesAcked.getOrDefault(deviceName, 0);
      Integer targetPulses = deviceTargetPulses.get(deviceName);
      Set<Integer> seenPulses = deviceSeenPulseNumbers.get(deviceName);
      int actualPulseCount = (seenPulses != null) ? seenPulses.size() : 0;

      android.util.Log.w(TAG, "⏱️ Final status: " + actualPulseCount + "/" + targetPulses +
                        " pulses collected, " + acked + "/" + sent + " batches acknowledged");

      // Check if data is incomplete and attempt retry before completing
      if (targetPulses != null && actualPulseCount < targetPulses) {
        android.util.Log.w(TAG, "⏱️ Incomplete data detected (" + actualPulseCount + "/" + targetPulses +
                          "), attempting retry from timeout path");

        if (tryRetryMissingBatches(deviceName, seenPulses, targetPulses)) {
          android.util.Log.i(TAG, "🔄 Retry initiated from inactivity timeout path");
          return; // Exit without completing - wait for retry results
        } else {
          android.util.Log.w(TAG, "⏱️ Retry not possible or already attempted, completing with incomplete data");
        }
      }

      android.util.Log.w(TAG, "⏱️ Completing log collection after inactivity timeout");
      completeLogCollection(deviceName);
    };

    // Store and schedule the new timer
    deviceCompletionTimeoutRunnables.put(deviceName, inactivityTimer);
    timeoutHandler.postDelayed(inactivityTimer, INACTIVITY_TIMEOUT_MS);
  }

  /**
   * Check if log collection can be completed based on ACK status
   * Called when: (1) all pulses received from device, or (2) JS acknowledges a batch
   */
  private void checkLogCollectionCompletion(String deviceName) {
    Integer targetPulses = deviceTargetPulses.get(deviceName);

    if (targetPulses == null) {
      return; // Not in log collection mode
    }

    // FIX: Check if log collection was cancelled - don't complete if it was
    // This prevents completion logic from running after user cancels, which would
    // trigger unwanted upload modal on wrong screen
    if (!deviceLoggingActive.getOrDefault(deviceName, false)) {
      android.util.Log.i(TAG, "⏭️ SKIP completion check - collection was cancelled for: " + deviceName);
      return;
    }

    // Calculate expected total number of batches
    int expectedBatches = (int) Math.ceil((double) targetPulses / MAX_PULSES_PER_FETCH);

    // Check JS acknowledgment status - this is the source of truth
    // FIX #10: Convert batch NUMBER to COUNT for both sent and acked
    // Both devicePulseBatchesSent and devicePulseBatchesAcked store LAST batch NUMBER (0, 1, 2... N-1)
    // We need the COUNT of batches for comparison with expectedBatches
    // Examples: Batch 0 stored → -1+1 = 1 count, Batch 257 stored → 257+1 = 258 count
    int sent = devicePulseBatchesSent.getOrDefault(deviceName, -1) + 1;
    int acked = devicePulseBatchesAcked.getOrDefault(deviceName, -1) + 1;

    // Get actual pulse count for logging
    Set<Integer> seenPulses = deviceSeenPulseNumbers.get(deviceName);
    int actualPulseCount = (seenPulses != null) ? seenPulses.size() : 0;

    android.util.Log.d(TAG, "🔍 Completion check: pulses=" + actualPulseCount +
                      "/" + targetPulses + ", batches ACKed=" + acked + "/" + sent +
                      "/" + expectedBatches + " (expected)");

    // ✅ FIX: Only complete when ALL expected batches have been sent AND acknowledged
    // Previous bug: checked (sent > 0 && acked >= sent) which completed after first batch
    // This caused premature completion with only 7/1800 pulses (batch #1 only)
    if (sent >= expectedBatches && acked >= expectedBatches) {
      // ✅ All expected batches sent AND acknowledged by JS
      android.util.Log.i(TAG, "✅ ACK-driven completion: All " + expectedBatches + " batches sent and acknowledged, " +
                        actualPulseCount + " pulses collected");

      // Cancel timeout timer since we're completing via ACK
      // This cancels either the inactivity timer (if ACKs were flowing) or absolute timeout (if no ACKs yet)
      Runnable timeoutRunnable = deviceCompletionTimeoutRunnables.remove(deviceName);
      if (timeoutRunnable != null) {
        timeoutHandler.removeCallbacks(timeoutRunnable);
        android.util.Log.d(TAG, "⏱️ Cancelled timeout timer (ACK-driven completion)");
      }

      // Check if we have 100% complete data
      double completeness = (actualPulseCount * 100.0) / targetPulses;

      if (completeness == 100.0) {
        // Perfect! Complete immediately
        android.util.Log.i(TAG, "✅ 100% complete data - finishing now");
        completeLogCollection(deviceName);
      } else {
        // We got all ACKs but data is incomplete - try retry
        android.util.Log.w(TAG, "⚠️ All ACKs received but only " +
                          String.format("%.1f", completeness) + "% data (" +
                          actualPulseCount + "/" + targetPulses + " pulses)");

        if (tryRetryMissingBatches(deviceName, seenPulses, targetPulses)) {
          // Retry initiated - let ACK system handle completion naturally
          // If retry batches arrive → ACKs → inactivity timer resets → eventually completes
          // If retry batches don't arrive → no ACKs → inactivity timer fires → completes
          android.util.Log.i(TAG, "🔄 Retry initiated - will complete via inactivity timer");
          // Don't call completeLogCollection() here - let timer system handle it
        } else {
          // No retry possible/needed - complete now
          android.util.Log.i(TAG, "⏭️ No retry possible - completing with incomplete data");
          completeLogCollection(deviceName);
        }
      }
    } else {
      android.util.Log.d(TAG, "⏳ Waiting for all batches: " + acked + "/" + expectedBatches + " acknowledged");
      // Inactivity timer will trigger completion 10s after last ACK (or absolute timeout if no ACKs)
    }
  }

  /**
   * Attempt to retry missing batches when data is incomplete
   * Called from:
   * 1. checkLogCollectionCompletion - when all ACKs received but data incomplete
   * 2. inactivityTimer - when timeout occurs with incomplete data
   *
   * @param deviceName Device identifier
   * @param seenPulses Set of pulse numbers already received
   * @param targetPulses Total expected pulse count
   * @return true if retry initiated, false if no retry possible/needed
   */
  private boolean tryRetryMissingBatches(String deviceName, Set<Integer> seenPulses, Integer targetPulses) {
    try {
      // Verify collection still active
      if (!deviceLoggingActive.getOrDefault(deviceName, false)) {
        android.util.Log.d(TAG, "⏭️ Retry skipped - collection no longer active");
        return false;
      }

      // Check retry count
      int retryCount = deviceRetryCount.getOrDefault(deviceName, 0);
      if (retryCount >= MAX_RETRY_ATTEMPTS) {
        android.util.Log.d(TAG, "⏭️ Retry skipped - max retries (" + MAX_RETRY_ATTEMPTS + ") reached");
        return false;
      }

      if (seenPulses == null || targetPulses == null || targetPulses == 0) {
        android.util.Log.w(TAG, "⚠️ Retry skipped - invalid pulse data");
        return false;
      }

      // Find missing batches
      List<Integer> missingBatches = findMissingBatchIndices(seenPulses, targetPulses);

      if (missingBatches.isEmpty()) {
        android.util.Log.w(TAG, "⚠️ No specific missing batches identified");
        return false;
      }

      // Safety: Don't retry if too many missing (>30% indicates hardware failure)
      int totalBatches = (int) Math.ceil((double) targetPulses / MAX_PULSES_PER_FETCH);
      if (missingBatches.size() > totalBatches * 0.3) {
        android.util.Log.e(TAG, "❌ Too many missing batches (" +
                          missingBatches.size() + "/" + totalBatches + " = " +
                          String.format("%.1f", (missingBatches.size() * 100.0 / totalBatches)) +
                          "%) - likely hardware failure, not retrying");
        return false;
      }

      android.util.Log.w(TAG, "🔄 RETRY #" + (retryCount + 1) + ": Re-requesting " +
                        missingBatches.size() + " missing batch(es)");

      // Increment retry counter
      deviceRetryCount.put(deviceName, retryCount + 1);

      // Queue retry commands for missing batches with 200ms spacing
      for (int i = 0; i < missingBatches.size(); i++) {
        int batchStart = missingBatches.get(i);
        int remainingPulses = targetPulses - batchStart;
        int actualPulses = Math.min(MAX_PULSES_PER_FETCH, remainingPulses);

        final int index = batchStart;
        final int pulses = actualPulses;
        int delayMs = i * LOG_COMMAND_INTERVAL_MS; // 200ms per command

        timeoutHandler.postDelayed(() -> {
          sendLogReadCommand(deviceName, index, pulses, targetPulses);
        }, delayMs);
      }

      int totalRetryTime = missingBatches.size() * LOG_COMMAND_INTERVAL_MS;
      android.util.Log.i(TAG, "⏱️ Retry scheduled: " + missingBatches.size() +
                        " commands over ~" + totalRetryTime + "ms");
      android.util.Log.i(TAG, "⏱️ Inactivity timer will fire if no ACKs within 10s of last retry response");

      return true; // Retry initiated

    } catch (Exception e) {
      android.util.Log.e(TAG, "❌ Retry error: " + e.getMessage());
      return false; // On error, don't retry
    }
  }

  /**
   * Find missing batch start indices by checking pulse number gaps
   * Returns list of batch start indices that have at least one missing pulse
   *
   * @param seenPulses Set of pulse numbers already received
   * @param totalPulses Total expected pulse count
   * @return List of batch start indices with missing pulses
   */
  private List<Integer> findMissingBatchIndices(Set<Integer> seenPulses, int totalPulses) {
    List<Integer> missingBatches = new ArrayList<>();

    // Check each batch for missing pulses
    for (int batchStart = 0; batchStart < totalPulses; batchStart += MAX_PULSES_PER_FETCH) {
      int batchEnd = Math.min(batchStart + MAX_PULSES_PER_FETCH, totalPulses);
      boolean hasMissing = false;

      // Check if any pulse in this batch is missing
      for (int pulseNum = batchStart; pulseNum < batchEnd; pulseNum++) {
        if (!seenPulses.contains(pulseNum)) {
          hasMissing = true;
          break;
        }
      }

      if (hasMissing) {
        missingBatches.add(batchStart);
        android.util.Log.d(TAG, "🔍 Missing batch detected at index " + batchStart +
                          " (pulses " + batchStart + "-" + (batchEnd - 1) + ")");
      }
    }

    android.util.Log.i(TAG, "📊 Gap analysis: " + missingBatches.size() +
                      " missing batch(es) out of " +
                      ((totalPulses + MAX_PULSES_PER_FETCH - 1) / MAX_PULSES_PER_FETCH) + " total");

    return missingBatches;
  }

  /**
   * React Method: JS calls this after processing each pulse batch
   * Implements ACK protocol for reliable event delivery
   *
   * @param deviceName Device identifier
   * @param batchSequence Batch sequence number being acknowledged
   */
  @ReactMethod
  public void acknowledgePulseBatch(int batchSequence) {
    if (heartbeatDevice == null) return;
    try {
      // FIX #9: Initialize to -1 to accept batch 0 (first batch)
      // Previous: getOrDefault(deviceName, 0) rejected batch 0 since 0 != 0+1
      // Fixed: getOrDefault(deviceName, -1) accepts batch 0 since 0 == -1+1
      int currentAck = devicePulseBatchesAcked.getOrDefault(heartbeatDevice, -1);

      // Only update if this is the next expected sequence (prevents out-of-order ACKs)
      if (batchSequence == currentAck + 1) {
        devicePulseBatchesAcked.put(heartbeatDevice, batchSequence);
        android.util.Log.d(TAG, "✅ ACK received: batch #" + batchSequence + " from " + heartbeatDevice);

        // FIX #5: Emit progress based on ACKs received (actual data confirmed by JS)
        int sent = devicePulseBatchesSent.getOrDefault(heartbeatDevice, 0);
        int totalPulses = deviceTargetPulses.getOrDefault(heartbeatDevice, 0);

        // FIX #11: Calculate progress based on batches ACKed by JavaScript, not total pulses in Set
        // ISSUE: seenPulses.size() includes ALL pulses native has processed (including batches
        // sent BEFORE JavaScript received the START event due to React Native bridge latency).
        // This causes progress to jump from 0% to ~16% because native processes ~43 batches
        // during the ~8 second bridge delay before JavaScript receives the START event.
        // SOLUTION: Use batchSequence (the batch being ACKed) to calculate confirmed pulse count.
        // Each batch contains exactly 7 pulses, so batch #0 = 7 pulses, batch #1 = 14 pulses, etc.
        int pulsesConfirmedByJS = (batchSequence + 1) * 7;  // batchSequence is 0-indexed
        // Cap at totalPulses to prevent progress > 100% (e.g., 1800 pulses = 258 batches,
        // but batch #257 would calculate 1806 pulses = 100.33%)
        int actualPulseCount = Math.min(pulsesConfirmedByJS, totalPulses);
        double progress = totalPulses > 0 ? (actualPulseCount * 100.0) / totalPulses : 0;

        WritableMap progressParams = Arguments.createMap();
        progressParams.putString("deviceName", heartbeatDevice);
        progressParams.putString("status", "IN_PROGRESS");
        progressParams.putInt("currentIndex", actualPulseCount);  // Use actual pulse count
        progressParams.putInt("totalPulses", totalPulses);
        progressParams.putDouble("progress", progress);
        eventEmit("onNativeLogProgress", progressParams);

        // Reset inactivity timer on every ACK (10-second window from last ACK)
        resetInactivityTimer(heartbeatDevice);

        // Check if we can complete now that this batch is ACKed
        checkLogCollectionCompletion(heartbeatDevice);
      } else if (batchSequence <= currentAck) {
        android.util.Log.w(TAG, "⚠️ Duplicate ACK: batch #" + batchSequence +
                          " (already at #" + currentAck + ")");
      } else {
        android.util.Log.w(TAG, "⚠️ Out-of-order ACK: got #" + batchSequence +
                          ", expected #" + (currentAck + 1));
      }
    } catch (Exception e) {
      android.util.Log.e(TAG, "❌ Error processing ACK for batch #" + batchSequence + ": " + e.getMessage());
    }
  }

  /**
   * Complete log collection - let React code handle device reset via SerialUtils
   */
  private void completeLogCollection(String deviceName) {
    try {
      android.util.Log.i(TAG, "✅ COMPLETE: Log collection finished for: " + deviceName);

      // Log final ACK statistics
      int sent = devicePulseBatchesSent.getOrDefault(deviceName, 0);
      int acked = devicePulseBatchesAcked.getOrDefault(deviceName, 0);
      android.util.Log.i(TAG, "📊 Batch stats: " + acked + "/" + sent + " acknowledged by JS");

      // Get final counts for completion event
      int currentIndex = deviceLogIndex.getOrDefault(deviceName, 0);
      int totalPulses = deviceTargetPulses.getOrDefault(deviceName, 0);

      // FIX #5: Diagnostic logging - verify completion accuracy
      // Compare all three pulse count sources to detect mismatches
      // Before Fix #2: deviceLogIndex would always be 0 (never updated)
      // After Fix #2: deviceLogIndex should equal seenPulses.size() and totalPulses
      Set<Integer> finalSeenPulses = deviceSeenPulseNumbers.get(deviceName);
      int actualPulseCount = (finalSeenPulses != null) ? finalSeenPulses.size() : 0;
      android.util.Log.i(TAG, "📊 Completion counts: deviceLogIndex=" + currentIndex +
                        ", seenPulses.size=" + actualPulseCount +
                        ", targetPulses=" + totalPulses);

      // PHASE 1: Detect incomplete data (threshold: 95% minimum for medical compliance)
      boolean hasIncompleteData = false;
      double completeness = 0.0;
      if (totalPulses > 0) {
        completeness = (actualPulseCount * 100.0) / totalPulses;
        hasIncompleteData = completeness < 95.0;
      }

      if (hasIncompleteData) {
        android.util.Log.e(TAG, "❌ INCOMPLETE DATA DETECTED: " + actualPulseCount + "/" +
                          totalPulses + " pulses (" + String.format("%.1f", completeness) + "%)");
      }

      // Alert if counts don't match (indicates Fix #2 isn't working or incomplete collection)
      if (currentIndex != actualPulseCount || currentIndex != totalPulses) {
        android.util.Log.e(TAG, "❌ MISMATCH DETECTED: deviceLogIndex=" + currentIndex +
                          ", seenPulses=" + actualPulseCount +
                          ", targetPulses=" + totalPulses +
                          " (should all be equal!)");
      }

      // Emit COMPLETE status for TreatmentMode useEffect monitoring
      WritableMap completeParams = Arguments.createMap();
      completeParams.putString("deviceName", deviceName);
      completeParams.putString("status", "COMPLETE");
      completeParams.putInt("currentIndex", currentIndex);
      completeParams.putInt("totalPulses", totalPulses);
      completeParams.putDouble("progress", 100.0);
      // PHASE 1: Add incomplete data information
      completeParams.putBoolean("hasIncompleteData", hasIncompleteData);
      completeParams.putDouble("completeness", completeness);
      completeParams.putInt("collected", actualPulseCount);
      completeParams.putInt("expected", totalPulses);
      completeParams.putInt("retryAttempts", deviceRetryCount.getOrDefault(deviceName, 0));
      eventEmit("onNativeLogStatusChange", completeParams);

      // Clean up ACK tracking state
      devicePulseBatchesSent.remove(deviceName);
      devicePulseBatchesAcked.remove(deviceName);
      deviceCompletionTimeoutRunnables.remove(deviceName);
      deviceRetryCount.remove(deviceName);

      // CRITICAL FIX: Resume heartbeat FIRST to prevent 700ms gap causing USB reset
      // The device firmware triggers USB hub reset if no commands received for ~700ms
      // CRITICAL: Clear pulse tracking (clearPulseTracking=true) on successful completion
      // This resets deduplication state for next treatment session
      cleanupLogCollectionState(deviceName, true, true);
      android.util.Log.i(TAG, "✅ COMPLETE: Heartbeat resumed immediately to prevent USB timeout");

      // THEN send reset command (goes through queue normally after heartbeat active)
      nativeResetDeviceLog(deviceName);

    } catch (Exception e) {
      android.util.Log.e(TAG, "❌ COMPLETE: Error: " + e.getMessage());
      // Force cleanup with pulse tracking clear
      cleanupLogCollectionState(deviceName, true, true);
    }
  }

  /**
   * Native device log reset - clears device log buffer after collection
   * Migrated from LoggingEngine.resetDeviceLog()
   */
  private void nativeResetDeviceLog(String deviceName) {
    try {
      android.util.Log.i(TAG, "Resetting device log buffer for: " + deviceName);

      // Build reset command: [WRITE_CMD, ADDRESS_3_BYTES, SIZE_1_BYTE, RESET_VALUE_1_BYTE]
      byte[] command = new byte[6];
      command[0] = (byte) COMMAND_WRITE;

      // Convert LOG_RESET_ADDRESS to 3 bytes
      byte[] addressBytes = toThreeBytes(LOG_RESET_ADDRESS);
      System.arraycopy(addressBytes, 0, command, 1, 3);

      // Size and reset value
      command[4] = (byte) 0x01; // Size: 1 byte
      command[5] = (byte) 0x01; // Reset value: 1

      // Send reset command to device
      writeSerialportBytes(deviceName, command);
      android.util.Log.i(TAG, "Device log reset command sent successfully to: " + deviceName);
    } catch (Exception e) {
      android.util.Log.e(TAG, "Error resetting device log for " + deviceName + ": " + e.getMessage());
    }
  }

  /**
   * React Native method to reset device log buffer
   * Called when skipping sequence without session data or cancelling collection
   */
  @ReactMethod
  public void resetDeviceLog() {
    if (heartbeatDevice == null) return;
    try {
      android.util.Log.i(TAG, "Manual device log reset requested for: " + heartbeatDevice);
      nativeResetDeviceLog(heartbeatDevice);
    } catch (Exception e) {
      android.util.Log.e(TAG, "Error resetting device log: " + e.getMessage());
    }
  }

  /**
   * React Native method to manually start log collection (for manual finish)
   * Native-first: Uses current device state instead of React parameters
   */
  @ReactMethod
  public void startManualLogCollection() {
    if (heartbeatDevice == null) return;
    try {
      // Native-first: Get current lastPulseIndex from device processing state
      int lastPulseIndex = lastProcessedPulseIndex > 0 ? lastProcessedPulseIndex : 0;

      // CONSISTENCY FIX: Always emit START event first (like autoStartLogCollection does)
      // This ensures JavaScript receives START → COMPLETE for ALL cases (0 or N pulses)
      WritableMap startParams = Arguments.createMap();
      startParams.putString("deviceName", heartbeatDevice);
      startParams.putString("status", "START");
      eventEmit("onNativeLogStatusChange", startParams);

      if (lastPulseIndex <= 0) {
        android.util.Log.w(TAG, "Manual log collection - no pulses available for: " + heartbeatDevice + ", emitting immediate completion event");
        // Emit completion event immediately after START for 0 pulses
        WritableMap completeParams = Arguments.createMap();
        completeParams.putString("deviceName", heartbeatDevice);
        completeParams.putString("status", "COMPLETE");
        completeParams.putInt("currentIndex", 0);
        completeParams.putInt("totalPulses", 0);
        completeParams.putDouble("progress", 100.0);
        eventEmit("onNativeLogStatusChange", completeParams);
        return;
      }

      android.util.Log.i(TAG, "Manual log collection requested for: " + heartbeatDevice + " with " + lastPulseIndex + " pulses");

      autoStartLogCollection(heartbeatDevice, lastPulseIndex);
    } catch (Exception e) {
      android.util.Log.e(TAG, "Error in manual log collection: " + e.getMessage());
      emitLogError(heartbeatDevice, "Failed to start manual log collection");
    }
  }

  /**
   * React Native method to suppress the next auto-log collection for a device.
   * Call BEFORE sending the stop/discard command so that when the native layer
   * detects treatmentStatus 1→0 it skips autoStartLogCollection.
   */
  @ReactMethod
  public void suppressNextLogCollection() {
    if (heartbeatDevice == null) return;
    android.util.Log.i(TAG, "🚫 suppressNextLogCollection set for: " + heartbeatDevice);
    deviceSuppressNextLogCollection.put(heartbeatDevice, true);
  }

  /**
   * React Native method to stop log collection (for cancellation)
   */
  @ReactMethod
  public void stopNativeLogging() {
    if (heartbeatDevice == null) return;
    try {
      android.util.Log.i(TAG, "🛑 CANCEL: Stopping log collection for: " + heartbeatDevice);

      // Check if actually collecting for this device
      if (!deviceLoggingActive.getOrDefault(heartbeatDevice, false)) {
        android.util.Log.w(TAG, "⚠️ CANCEL: No active collection for: " + heartbeatDevice);
        return;
      }

      // Emit CANCELLED status BEFORE cleanup
      WritableMap params = Arguments.createMap();
      params.putString("deviceName", heartbeatDevice);
      params.putString("status", "CANCELLED");
      eventEmit("onNativeLogStatusChange", params);

      // CRITICAL: Clean up and resume heartbeat
      // CRITICAL: Clear pulse tracking (clearPulseTracking=true) on manual cancel
      // This resets deduplication state since user explicitly cancelled collection
      cleanupLogCollectionState(heartbeatDevice, true, true);

      // Reset device log buffer to prevent stale data
      nativeResetDeviceLog(heartbeatDevice);

      android.util.Log.i(TAG, "✅ CANCEL: Log collection stopped, device log reset, and heartbeat resumed");

    } catch (Exception e) {
      android.util.Log.e(TAG, "❌ CANCEL: Error stopping log collection: " + e.getMessage());
      // Force cleanup even on error with pulse tracking clear
      cleanupLogCollectionState(heartbeatDevice, true, true);
    }
  }

    @ReactMethod
    public void loadAutoRampTimeline(ReadableArray timelineData, double targetMaxPercent, Promise promise) {
        if (heartbeatDevice == null) { promise.resolve(false); return; }
        try {
            autoRampEngine.loadTimeline(heartbeatDevice, timelineData, targetMaxPercent);
            promise.resolve(true);
        } catch (Exception e) {
            android.util.Log.e(TAG, "Failed to load auto-ramp timeline: " + e.getMessage(), e);
            promise.reject("AUTO_RAMP_ERROR", "Failed to load timeline: " + e.getMessage(), e);
        }
    }

    @ReactMethod
    public void advanceAutoRampSequence() {
        if (heartbeatDevice == null) return;
        autoRampEngine.advanceSequence(heartbeatDevice);
    }

    @ReactMethod
    public void clearAutoRamp() {
        if (heartbeatDevice == null) return;
        autoRampEngine.clear(heartbeatDevice);
    }

    @ReactMethod
    public void setAutoRampActive(boolean active) {
        if (heartbeatDevice == null) return;
        autoRampEngine.setActive(heartbeatDevice, active);
    }

    @ReactMethod
    public void resetTimestampValidator() {
        timestampValidator.clear();
        android.util.Log.i(TAG, "TIMESTAMP_VALIDATOR: Cleared by JS (sequence repeat)");
    }

  /**
   * Emit log collection error
   */
  private void emitLogError(String deviceName, String errorMessage) {
    WritableMap errorParams = Arguments.createMap();
    errorParams.putString("deviceName", deviceName);
    errorParams.putString("status", "ERROR");
    errorParams.putString("message", errorMessage);
    eventEmit("onNativeLogError", errorParams);
  }
}
