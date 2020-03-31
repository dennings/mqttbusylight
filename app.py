import paho.mqtt.client as mqtt
import os, asyncio, threading, random, time, datetime
from dotenv import load_dotenv
from azure.iot.device.aio import IoTHubDeviceClient, ProvisioningDeviceClient
from azure.iot.device import MethodResponse

load_dotenv()

broker_address = os.getenv('BROKER_ADDRESS')     # grab config from .env file
broker_user = os.getenv('BROKER_USER')
broker_passwd = os.getenv('BROKER_PASSWD')
mqtt_cmnd_topic = os.getenv('MQTT_CMND_TOPIC')   # i.e. Tasmota BULB -> cmnd/gosungbulb4/COLOR
mqtt_stat_topic = os.getenv('MQTT_STAT_TOPIC')   # i.e. Tasmota BULB -> tele/gosungbulb4/STATE
mqtt.Client.connected_flag=False                 #create flag in class

id_scope = os.getenv('ID_SCOPE')
device_id = os.getenv('DEVICE_ID')
primary_key = os.getenv('PRIMARY_KEY')

def on_mqtt_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True               #set connected_flag
        print("MQTT: connected OK")
    else:
        print("MQTT: Bad connection Returned code=",rc)

def on_mqtt_publish(client, userdata, result):   #create function for callback
    print("MQTT: data published")

def set_colour(colour,mqttclient,mqtt_cmnd_topic):
    r = '0x' + colour[0:2]
    g = '0x' + colour[2:4]
    b = '0x' + colour[4:6]

    r_value = int(r, 0)
    g_value = int(g, 0)
    b_value = int(b, 0)

    now = datetime.datetime.now()
    print('Updating color: r =', r_value, ', g =', g_value, ', b =', b_value)
    mqttclient.publish(mqtt_cmnd_topic, colour[0:6])
    print(now.strftime("%Y-%m-%d %H:%M:%S")+': mqtt set.',colour[0:6])

async def main():
    # provision the device
    async def register_device():
        provisioning_device_client = ProvisioningDeviceClient.create_from_symmetric_key(
            provisioning_host='global.azure-devices-provisioning.net',
            registration_id=device_id,
            id_scope=id_scope,
            symmetric_key=primary_key,
        )

        return await provisioning_device_client.register()

    results = await asyncio.gather(register_device())
    registration_result = results[0]

    # build the connection string
    conn_str='HostName=' + registration_result.registration_state.assigned_hub + \
                ';DeviceId=' + device_id + \
                ';SharedAccessKey=' + primary_key

    # The client object is used to interact with your Azure IoT Central.
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)

    # connect the client.
    print('Azure IoT Central: Connecting')
    await device_client.connect()
    print('Azure IoT Central: Connected')

    mqttclient = mqtt.Client("Pi0")        #create new instance
    mqttclient.on_connect=on_mqtt_connect  #bind call back function
    mqttclient.on_publish=on_mqtt_publish  #assign function to callback
    mqttclient.loop_start()
    mqttclient.username_pw_set(username=broker_user,password=broker_passwd)
    print("MQTT: Connecting to broker ",broker_address)
    mqttclient.connect(broker_address)         #connect to broker
    while not mqttclient.connected_flag:       #wait in loop
        print("In wait loop")
        time.sleep(1)
    print("MQTT: send AAAAAA COLOR to bulb")
    mqttclient.publish("cmnd/gosungbulb4/COLOR","AAAAAA")
    print("MQTT: turn OFF light")
    mqttclient.publish("cmnd/gosungbulb4/COLOR","000000")

    # Get the current colour
    twin = await device_client.get_twin()
    print('Got twin: ', twin)

    try:
        desired_colour = twin['reported']['colour']
        set_colour(desired_colour,mqttclient,mqtt_cmnd_topic)
    except:
        print("Couldn't load twin ;(")

    # listen for commands
    async def command_listener(device_client):
        while True:
            method_request = await device_client.receive_method_request()

            now = datetime.datetime.now()
            print('Call made to', method_request.name)
            colour = method_request.payload
            print(now.strftime("%Y-%m-%d %H:%M:%S"),': payload', colour)

            if isinstance(colour, dict):
                colour = colour['colour']

            print('payload2', colour)
            set_colour(colour,mqttclient,mqtt_cmnd_topic)    # set colour to BULB

            payload = {'result': True, 'data': colour}
            method_response = MethodResponse.create_from_method_request(
                method_request, 200, payload
            )
            await device_client.send_method_response(method_response)

            # Write the colour back as a property
            await device_client.patch_twin_reported_properties({'colour':colour})

    # async loop that controls the lights
    async def main_loop():
        global mode
        while True:
            await asyncio.sleep(1)

    listeners = asyncio.gather(command_listener(device_client))

    await main_loop()

    # Cancel listening
    listeners.cancel()

    # Finally, disconnect
    await device_client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
