# react-native-usb-serialport

[![npm version](http://img.shields.io/npm/v/react-native-usb-serialport.svg?style=flat-square)](https://npmjs.org/package/react-native-usb-serialport "View this project on npm")
[![npm downloads](http://img.shields.io/npm/dm/react-native-usb-serialport.svg?style=flat-square)](https://npmjs.org/package/react-native-usb-serialport "View this project on npm")
[![npm licence](http://img.shields.io/npm/l/react-native-usb-serialport.svg?style=flat-square)](https://npmjs.org/package/react-native-usb-serialport "View this project on npm")
[![Platform](https://img.shields.io/badge/platform-android-989898.svg?style=flat-square)](https://npmjs.org/package/react-native-usb-serialport "View this project on npm")

USB serial port module for the AMPA TMS app. The usb serial port part includes the `.aar` library from [felHR85/UsbSerial](https://github.com/felHR85/UsbSerial).

### Removed: TCP socket and gateway code
The upstream fork shipped TCP/TLS socket code copied from [react-native-tcp-socket@5.2.1](https://github.com/Rapsssito/react-native-tcp-socket/tree/v5.2.1) and a socket-to-serial gateway. Nothing in the app used either, so both were deleted, including a trust-all TLS path and a TLS client without hostname verification. Do not bring them back when syncing upstream. If the app ever needs TCP, depend on `react-native-tcp-socket` directly.

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
