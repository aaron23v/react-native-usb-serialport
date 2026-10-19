# react-native-usb-serialport

[![npm version](http://img.shields.io/npm/v/react-native-usb-serialport.svg?style=flat-square)](https://npmjs.org/package/react-native-usb-serialport "View this project on npm")
[![npm downloads](http://img.shields.io/npm/dm/react-native-usb-serialport.svg?style=flat-square)](https://npmjs.org/package/react-native-usb-serialport "View this project on npm")
[![npm licence](http://img.shields.io/npm/l/react-native-usb-serialport.svg?style=flat-square)](https://npmjs.org/package/react-native-usb-serialport "View this project on npm")
[![Platform](https://img.shields.io/badge/platform-android-989898.svg?style=flat-square)](https://npmjs.org/package/react-native-usb-serialport "View this project on npm")

High performance JAVA native gateway passthrough between multi usb serial port and multi socket without JS bridge, or just use multi usb serial port or socket alone.

The time `socket -> serial port -> socket`:

* JAVA native gateway takes 8ms
* JS bridge gateway takes 300ms

The usb serial port part include `.aar` library from [felHR85/UsbSerial](https://github.com/felHR85/UsbSerial)

The socket part copy and modify code from [react-native-tcp-socket@5.2.1](https://github.com/Rapsssito/react-native-tcp-socket/tree/v5.2.1)

### Local modifications to the socket code (keep when re-syncing upstream)
- Removed `BlindTrustManager`, `SSLCertificateHelper.createBlindSocketFactory()` and the `tlsCheckValidity` connection option. TLS connections always validate the server certificate, against either the system trust store or the CA passed in `tlsCert`. Don't bring the trust-all path back when merging newer react-native-tcp-socket code.

## Changelog
### [3.0.0] - 2024-08-23
Support android targeting S+

Fix `Direct local .aar file dependencies are not supported when building an AAR` when assemble release in gradle-6.7.1 or higher.

### [2.3.0] - 2021-11-05
Remove executorService usage in writeSocketBytes, thus JAVA native gateway can have more smoother socket response rate.

### [2.2.0] - 2021-09-01
High performance JAVA native gateway passthrough between socket and serialport without JS bridge

### [2.1.0] - 2021-01-22
USB device access pop-up suppression

### [2.0.0] - 2021-01-22
Add multi usb serial port connections support

## Install

Install `react-native-usb-serialport@3.x` in `gradle-6.7.1` and add below into your `APP/android/app/build.gradle`
```
    implementation fileTree(dir: '../../node_modules/react-native-usb-serialport/android/libs', include: ['*.aar'])
```

Install `react-native-usb-serialport@2.x` in `gradle-6.2-`

## Gateway Example As Usage
### Gateway serial port and socket server side runs in your react native APP
```javascript
const {DeviceEventEmitter, Platform} = require('react-native');

import {RNSerialport, actions} from 'react-native-usb-serialport';
const TcpSocketCreateServer = require('react-native-usb-serialport/src/index.js')
  .createServer;
import _ from 'lodash';

class SerialportGateway {
  static tcpSocketPort = 7700;
  static tcpSocketServer = undefined;

  // JAVA native gateway need these
  static isNativeGateway = true;
  static isNativeGatewayJsEventEmitOnSerialportData = true; // won't reduce native gateway performance

  // JS bridge gateway need these and isNativeGateway should be false
  static jsAppBus2DeviceName = {};
  static jsDeviceName2Socket = {};

  static emitter =
    Platform.OS === 'ios'
      ? {
          addListener: () => {},
          removeListener: () => {},
          emit: () => {},
        }
      : DeviceEventEmitter;

  static doInit() {
    this.emitter.addListener(
      actions.ON_SERVICE_STARTED,
      this.onServiceStarted,
      this,
    );
    this.emitter.addListener(
      actions.ON_SERVICE_STOPPED,
      this.onServiceStopped,
      this,
    );
    this.emitter.addListener(
      actions.ON_DEVICE_ATTACHED,
      this.onDeviceAttached,
      this,
    );
    this.emitter.addListener(
      actions.ON_DEVICE_DETACHED,
      this.onDeviceDetached,
      this,
    );
    this.emitter.addListener(actions.ON_ERROR, this.onError, this);
    this.emitter.addListener(actions.ON_CONNECTED, this.onConnected, this);
    this.emitter.addListener(
      actions.ON_DISCONNECTED,
      this.onDisconnected,
      this,
    );
    this.emitter.addListener(actions.ON_READ_DATA, this.onReadData, this);
    // RNSerialport.setReturnedDataType(definitions.RETURNED_DATA_TYPES.HEXSTRING);
    Platform.OS === 'android' && RNSerialport.startUsbService();

    if (Platform.OS === 'android') {
      // who would build IoT project with ios :P
      this.tcpSocketServer = TcpSocketCreateServer((socket) => {
        console.warn(
          'connected client ' + socket.remoteAddress + ':' + socket.remotePort,
        );
        if (!this.isNativeGateway) {
          // setEncoding is meaningless when not isNativeGateway
          socket.setEncoding('binary');
        }

        socket.on('data', (rawData) => {
          if (this.isNativeGateway) {
            console.warn('should not be here ');
            return;
          }

          let data;
          try {
            data = JSON.parse(rawData);
          } catch (err) {
            data = [];
            [].map.call(rawData, (value, index, str) => {
              data.push(str.charCodeAt(index));
            });
          }
          if (__DEV__) {
            console.warn('from client ' + this.byteArray2HexArray(data));
          }

          let appBus = 0; // this example passthrough assume only one serial port, so 0 here, you can customize appBus from somewhere in the data received
          let deviceName = this.jsAppBus2DeviceName[appBus];
          if (deviceName) {
            this.jsDeviceName2Socket[deviceName] = socket;
            RNSerialport.writeBytes(deviceName, rawData);
          }
        });

        socket.on('error', (error) => {
          console.warn('client error ', error);
          // socket.end();
        });

        socket.on('close', (error) => {
          console.warn(
            'closed client ' + socket.remoteAddress + ':' + socket.remotePort,
          );
          _.omitBy(this.jsDeviceName2Socket, (value) => value === socket);
          // socket.destroy();
        });
      }).listen({port: this.tcpSocketPort});

      this.tcpSocketServer.on('error', (error) => {
        console.warn('server error ', error);
      });

      this.tcpSocketServer.on('close', () => {
        console.warn('server closed');
      });
    }
  }

  static doDestroy() {
    this.emitter.removeListener(
      actions.ON_SERVICE_STARTED,
      this.onServiceStarted,
      this,
    );
    this.emitter.removeListener(
      actions.ON_SERVICE_STOPPED,
      this.onServiceStopped,
      this,
    );
    this.emitter.removeListener(
      actions.ON_DEVICE_ATTACHED,
      this.onDeviceAttached,
      this,
    );
    this.emitter.removeListener(
      actions.ON_DEVICE_DETACHED,
      this.onDeviceDetached,
      this,
    );
    this.emitter.removeListener(actions.ON_ERROR, this.onError, this);
    this.emitter.removeListener(actions.ON_CONNECTED, this.onConnected, this);
    this.emitter.removeListener(
      actions.ON_DISCONNECTED,
      this.onDisconnected,
      this,
    );
    this.emitter.removeListener(actions.ON_READ_DATA, this.onReadData, this);

    if (Platform.OS === 'android') {
      RNSerialport.disconnectAllDevices();
      RNSerialport.stopUsbService();
      if (this.tcpSocketServer) {
        this.tcpSocketServer.destroy();
        this.tcpSocketServer = undefined;
      }
    }
  }

  static onServiceStarted(response) {
    console.warn('USB service started');

    if (this.isNativeGateway) {
      RNSerialport.setIsNativeGateway(this.isNativeGateway);
      RNSerialport.setIsNativeGatewayJsEventEmitOnSerialportData(
        this.isNativeGatewayJsEventEmitOnSerialportData,
      );
    }

    if (response.deviceAttached) {
      this.onDeviceAttached();
    }
  }

  static onServiceStopped() {
    console.warn('USB service stopped');
  }

  static onDeviceAttached(deviceName) {
    console.warn('USB device attached ' + deviceName);
    this.fillDeviceListAndConnect();
  }

  static onDeviceDetached(deviceName) {
    console.warn('USB device detached ' + deviceName);
  }

  static onConnected(deviceName) {
    console.warn(deviceName, 'USB serialPort connected');

    let busIndex = this.getBusIndexFromDevPath(deviceName);

    if (this.isNativeGateway) {
      RNSerialport.appBus2DeviceNamePut(busIndex, deviceName);
    } else {
      this.jsAppBus2DeviceName[busIndex] = deviceName;
    }
  }
  static onDisconnected(deviceName) {
    console.warn(deviceName, 'USB serialPort disconnected');

    if (this.isNativeGateway) {
      // will auto appBus2DeviceName.values().removeIf(deviceName::equals) in RNSerialportModule.java
    } else {
      _.omitBy(this.jsAppBus2DeviceName, (value) => value === deviceName);
    }
  }

  static onReadData(data) {
    if (__DEV__) {
      console.warn('onUsbSerialportReceiveData', {
        linuxDevPath: data.deviceName,
        valueArray: this.byteArray2HexArray(data.payload),
      });
    }

    if (this.isNativeGateway) {
      // will comes here when isNativeGatewayJsEventEmitOnSerialportData is true
      this.emitter.emit('YOUR_APP_MAYBE_ALSO_NEED_JS_onSerialportReceiveData', {
        linuxDevPath: data.deviceName,
        valueArray: data.payload,
      });
    } else {
      let socket = this.jsDeviceName2Socket[data.deviceName];
      if (socket) {
        socket.write(data.payload);
      }
    }
  }

  static onError(error) {
    console.error(error);
  }

  // deviceName of usb serial device on your board, maybe replaced in fillDeviceListAndConnect()
  static usbSerialPath = [
    '/dev/bus/usb/001/003',
    '/dev/bus/usb/001/005',
    '/dev/bus/usb/001/007',
    '/dev/bus/usb/001/009',
  ];

  static getDevPathFromBusIndex(busIndex) {
    return this.usbSerialPath[busIndex] || this.usbSerialPath[0];
  }

  static getBusIndexFromDevPath(devPath) {
    let index = this.usbSerialPath.indexOf(devPath);
    return index === -1 ? 0 : index;
  }

  static fillDeviceListAndConnect() {
    RNSerialport.getDeviceList().then((list) => {
      // console.warn(list);
      // [
      //     {
      //         "name": "/dev/bus/usb/001/010",
      //         "productId": 20752,
      //         "vendorId": 1578
      //     },
      //     {
      //         "name": "/dev/bus/usb/001/009",
      //         "productId": 29987,
      //         "vendorId": 6790 // ch34x
      //     },
      //     {
      //         "name": "/dev/bus/usb/001/005",
      //         "productId": 29987,
      //         "vendorId": 6790 // ch34x
      //     },
      //     {
      //         "name": "/dev/bus/usb/002/002",
      //         "productId": 46880,
      //         "vendorId": 3034
      //     },
      //     {
      //         "name": "/dev/bus/usb/001/003",
      //         "productId": 29987,
      //         "vendorId": 6790 // ch34x
      //     },
      //     {
      //         "name": "/dev/bus/usb/001/007",
      //         "productId": 29987,
      //         "vendorId": 6790 // ch34x
      //     }
      // ]

      let ch34xUsbSerialPath = [];
      list.map((item) => {
        if (item.vendorId === 6790) {
          ch34xUsbSerialPath.push(item.name);
        }
      });

      // If you use Beckhoff like module, pull and plug some modules,
      // their increased name should also be sorted.
      ch34xUsbSerialPath = ch34xUsbSerialPath.sort();

      ch34xUsbSerialPath.map((path, index) => {
        this.usbSerialPath[index] = path;
      });

      this.enableLinuxDev({
        devPaths: this.usbSerialPath,
      });
    });
  }

  static enableLinuxDev({devPaths}) {
    if (Platform.OS === 'android') {
      devPaths.map((path) => {
        RNSerialport.connectDevice(path, 115200);
      });
    }
  }

  static disableLinuxDev({devPaths}) {
    if (Platform.OS === 'android') {
      devPaths.map((path) => {
        RNSerialport.disconnectDevice(path);
      });
    }
  }

  static padHexString(string) {
    if (string.length === 1) {
      return '0' + string;
    } else {
      return string;
    }
  }

  static hexString2ByteArray(string) {
    let array = [];
    [].map.call(string, (value, index, str) => {
      if (index % 2 === 0) {
        array.push(parseInt(value + str[index + 1], 16));
      }
    });

    return array;
  }

  static byteArray2HexString(bytes) {
    return bytes
      .map((byte) => this.padHexString((byte & 0xff).toString(16)))
      .toString()
      .replace(/,/g, '')
      .toUpperCase();
  }

  static byteArray2HexArray(bytes) {
    return bytes
      .map((byte) => this.padHexString((byte & 0xff).toString(16)))
      .toString();
  }
}

module.exports = SerialportGateway;
```
### Gateway socket client side runs in nodejs
    HOST=192.168.1.108 PORT=7700 node app/utils/tcpSocketClientTest.js

* `192.168.1.108` is the IP address of your phone which runs APP
* `7700` equals tcpSocketPort inside `SerialportGateway.js` above
* `tcpSocketClientTest.js` is below
```javascript
var net = require('net');
var port = process.env.PORT || 7700;
var host = process.env.HOST || '127.0.0.1';
var client = new net.Socket();

function padHexString(string) {
  if (string.length === 1) {
    return '0' + string;
  } else {
    return string;
  }
}

function byteArray2HexArray(bytes) {
  return bytes
    .map((byte) => padHexString((byte & 0xff).toString(16)))
    .toString();
}

function sleepMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const DELAY_MS_SOCKET = 0;
let isBusy = true;

client.setEncoding('binary');

client.connect(port, host, async function () {
  console.log('connected server ' + host + ':' + port);

  for (let i = 0; i < 10; i++) {
    client.write(Buffer.from([0xaa, 0, 1, 2, 3, 4, 5, 6, 7, 0x55]));
    while (isBusy) {
      await sleepMs(DELAY_MS_SOCKET);
    }
    isBusy = true;
  }
  return;
});

client.on('data', function (rawData) {
  isBusy = false;
  let data;
  try {
    data = JSON.parse(rawData);
  } catch (err) {
    data = [];
    [].map.call(rawData, (value, index, str) => {
      data.push(str.charCodeAt(index));
    });
  }
  console.log('from server ' + byteArray2HexArray(data));
});

client.on('error', function (exception) {
  console.log('socket error ' + exception);
  client.destory();
});

client.on('close', function () {
  console.log('closed server ' + host + ':' + port);
});
```
## Customize JAVA native gateway
Write your own `YOUR_RN_PROJECT/app/utils/SerialportGateway.java`, and run below in `YOUR_RN_PROJECT/`:

    ln -sf ../../../../../../../../../app/utils/SerialportGateway.java node_modules/react-native-usb-serialport/android/src/main/java/com/melihyarikkaya/rnserialport/Gateway.java

## Documents of gateway
Ref to `Gateway Example As Usage` above with methods below:

    setIsNativeGateway
    setIsNativeGatewayJsEventEmitOnSerialportData
    appBus2DeviceNamePut

## Documents of socket part
Ref to `Gateway Example As Usage` above and [README of react-native-tcp-socket](https://github.com/Rapsssito/react-native-tcp-socket/blob/v5.2.1/README.md).

## Fork diff from [react-native-serialport](https://github.com/melihyarikkaya/react-native-serialport)
Add multi usb serial port connections support after forked from [react-native-serialport](https://github.com/melihyarikkaya/react-native-serialport)

```javascript
-  $ npm install --save react-native-serialport
+  $ npm install --save react-native-usb-serialport

-  import { RNSerialport, definitions, actions } from "react-native-serialport";
+  import { RNSerialport, definitions, actions } from "react-native-usb-serialport";

-  RNSerialport.disconnect();
+  RNSerialport.disconnectDevice('/dev/bus/usb/001/007');

+  RNSerialport.disconnectAllDevices();

-  RNSerialport.isOpen();
+  RNSerialport.isOpen('/dev/bus/usb/001/007');

-  RNSerialport.writeBytes([0, 1, 2, 3]);
+  RNSerialport.writeBytes('/dev/bus/usb/001/007', [0, 1, 2, 3]);

-  RNSerialport.writeString('HELLO');
+  RNSerialport.writeString('/dev/bus/usb/001/007', 'HELLO');

-  RNSerialport.writeBase64('SEVMTE8=');
+  RNSerialport.writeBase64('/dev/bus/usb/001/007', 'SEVMTE8=');

-  RNSerialport.writeHexString('48454C4C4F');
+  RNSerialport.writeHexString('/dev/bus/usb/001/007', '48454C4C4F');
```

### [USB device access pop-up suppression](https://stackoverflow.com/questions/12388914/usb-device-access-pop-up-suppression/15151075#15151075)
Add the following android intent to `android/app/src/main/AndroidManifest.xml` so that permissions are remembered on android (VS not remembered by `usbManager.requestPermission()`)
```
<activity
    ...
    ...
    >
    <intent-filter>
        <action android:name="android.hardware.usb.action.USB_DEVICE_ATTACHED" />
    </intent-filter>
    <meta-data android:name="android.hardware.usb.action.USB_DEVICE_ATTACHED" android:resource="@xml/usb_device_filter" />
</activity>
```

And create a filter file in `android/app/src/main/res/xml/usb_device_filter.xml`
```
<?xml version="1.0" encoding="utf-8"?>

<resources>
    <!-- vendor-id 6790 means ch34x usb serial port -->
    <usb-device vendor-id="6790" product-id="29987" />
</resources>
```
The `vendor-id` and `product-id` here have to be given in decimal, and they can be get by `RNSerialport.getDeviceList().then(console.warn)`

## Documents of usb serial port part (note above `Fork diff`)
1. [Download & Installation](https://github.com/melihyarikkaya/react-native-serialport/wiki/Download-&-Installation)
2. [Auto Connection](https://github.com/melihyarikkaya/react-native-serialport/wiki/Auto-Connection)
3. [Manual Connection](https://github.com/melihyarikkaya/react-native-serialport/wiki/Manual-Connection)
4. [Methods](https://github.com/melihyarikkaya/react-native-serialport/wiki/Methods)
5. [Error Descriptions](https://github.com/melihyarikkaya/react-native-serialport/wiki/Error-Descriptions)

## DEFAULT DEFINITIONS
| KEY                    | VALUE                                    |
|------------------------|------------------------------------------|
| RETURNED DATA TYPE     | INT ARRAY (Options: INTARRAY, HEXSTRING) |
| BAUND RATE             | 9600                                     |
| AUTO CONNECT BAUD RATE | 9600                                     |
| PORT INTERFACE         | -1                                       |
| DATA BIT               | 8                                        |
| STOP BIT               | 1                                        |
| PARITY                 | NONE                                     |
| FLOW CONTROL           | OFF                                      |
| DRIVER                 | AUTO                                     |

## Java Package Name
 _com.melihyarikkaya.rnserialport_

## Donate
To support my work, please consider donate.

- ETH: 0xd02fa2738dcbba988904b5a9ef123f7a957dbb3e

- <img src="https://raw.githubusercontent.com/flyskywhy/flyskywhy/main/assets/alipay_weixin.png" width="500">
