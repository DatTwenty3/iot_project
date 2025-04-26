import network
import uasyncio as asyncio
from machine import Pin, ADC, PWM
import dht
import ujson
import random
from umqtt.simple import MQTTClient

# MQTT Server Parameters
MQTT_CLIENT_ID = "esp32-iot-prj"
MQTT_BROKER    = "app.coreiot.io"
MQTT_USER      = "esp32-iot-prj"
MQTT_PASSWORD  = "esp32-iot-prj"
MQTT_TOPIC     = "v1/devices/me/telemetry"
RPC_TOPIC      = "v1/devices/me/rpc/request/+"

# Sensors
indoor_sensor  = dht.DHT22(Pin(15))  # Indoor DHT22 on pin 15
outdoor_sensor = dht.DHT22(Pin(16))  # Outdoor DHT22 on pin 16
mq2_sensor     = ADC(Pin(34))
mq2_sensor.atten(ADC.ATTN_11DB)

# Actuators
green_led       = Pin(26, Pin.OUT)
red_led         = Pin(27, Pin.OUT)
led_blink       = Pin(14, Pin.OUT)
gas_alert_led   = Pin(25, Pin.OUT)
servo           = PWM(Pin(18), freq=50)  # Servo at 50Hz

# Global state
indoor_temp      = None
indoor_hum       = None
outdoor_temp     = None
outdoor_hum      = None
current_gas      = None

gas_alert_active = False
blue_led_active  = False

# Window control state
window_state = False         # False=closed, True=open
free_window_control = True   # True=RPC control allowed, False=forced by gas

def rpc_callback(topic, msg):
    global free_window_control, window_state
    temp_data = {'value': True}
    try:
        jsonobj = ujson.loads(msg)
        method = jsonobj.get('method')
        params = jsonobj.get('params', True)

        if method == "setValueGreenLED":
            temp_data['value'] = params
            client.publish('v1/devices/me/attributes', ujson.dumps(temp_data), qos=1)
            green_led.value(1 if params else 0)

        elif method == "setValueRedLED":
            temp_data['value'] = params
            client.publish('v1/devices/me/attributes', ujson.dumps(temp_data), qos=1)
            red_led.value(1 if params else 0)
        
        elif method == "setValueWindow":
            # Only allow RPC when free_window_control is True
            if free_window_control:
                window_state = params
                servo.duty(26) if window_state else servo.duty(77)
            temp_data['value'] = params
            client.publish('v1/devices/me/attributes', ujson.dumps(temp_data), qos=1)

    except Exception as e:
        print("Error parsing JSON:", e)

async def wifi_connect():
    sta_if = network.WLAN(network.STA_IF)
    sta_if.active(True)
    sta_if.connect('Wokwi-GUEST', '')

    while not sta_if.isconnected():
        await asyncio.sleep(0.1)
    return sta_if

async def mqtt_connect(client):
    client.set_callback(rpc_callback)
    client.connect()
    client.subscribe(RPC_TOPIC)
    print('MQTT IS CONNECTED !!!')

async def mqtt_task(client):
    while True:
        client.check_msg()
        await asyncio.sleep(0.1)

async def gas_alert_blink_task():
    global gas_alert_active
    while True:
        if gas_alert_active:
            gas_alert_led.value(1)
            await asyncio.sleep(0.5)
            gas_alert_led.value(0)
            await asyncio.sleep(0.5)
        else:
            gas_alert_led.value(0)
            await asyncio.sleep(0.1)

async def blue_led_blink_task():
    global blue_led_active
    while True:
        if blue_led_active:
            led_blink.value(1)
            await asyncio.sleep(0.5)
            led_blink.value(0)
            await asyncio.sleep(0.5)
        else:
            led_blink.value(0)
            await asyncio.sleep(0.1)

async def sensor_task(client):
    global indoor_temp, indoor_hum, outdoor_temp, outdoor_hum, current_gas
    global gas_alert_active, blue_led_active, window_state, free_window_control
    pre_window_state = None
    prev_message = ""

    while True:
        # Measure indoor sensor
        indoor_sensor.measure()
        indoor_temp = indoor_sensor.temperature()
        indoor_hum  = indoor_sensor.humidity()

        # Measure outdoor sensor
        outdoor_sensor.measure()
        outdoor_temp = outdoor_sensor.temperature()
        outdoor_hum  = outdoor_sensor.humidity()

        # Read gas sensor
        gas_value = mq2_sensor.read()
        current_gas = gas_value
        
        # --- Window control based on gas ---
        if current_gas > 2020:
            gas_alert_active = True
            free_window_control = False
            window_state = True           # Force open when gas high
        else:
            gas_alert_active = False
            free_window_control = True    # Allow RPC control
            # window_state remains as set by RPC if any

        # Move servo according to window_state
        servo.duty(26) if window_state else servo.duty(77)

        # Publish window attribute when state changes
        if window_state != pre_window_state:
            client.publish('v1/devices/me/attributes', ujson.dumps({'state': window_state}), qos=1)
            pre_window_state = window_state

        # Blue LED based on indoor temperature
        blue_led_active = indoor_temp > 50

        # Publish telemetry
        message = ujson.dumps({
            "indoor_temp": indoor_temp,
            "indoor_hum": indoor_hum,
            "outdoor_temp": outdoor_temp,
            "outdoor_hum": outdoor_hum,
            "gas": current_gas
        })
        
        if message != prev_message:
            client.publish(MQTT_TOPIC, message)
            prev_message = message
        await asyncio.sleep(1)

async def battery_task(client):
    while True:
        battery_level = random.randint(1, 100)
        message = ujson.dumps({"battery": battery_level})
        client.publish(MQTT_TOPIC, message)
        await asyncio.sleep(20)

async def main():
    await wifi_connect()
    global client
    client = MQTTClient(MQTT_CLIENT_ID, MQTT_BROKER, user=MQTT_USER, password=MQTT_PASSWORD)
    await mqtt_connect(client)

    tasks = [
        mqtt_task(client),
        sensor_task(client),
        blue_led_blink_task(),
        battery_task(client),
        gas_alert_blink_task()
    ]
    await asyncio.gather(*tasks)

asyncio.run(main())
