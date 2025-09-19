import gzip
import os
from datetime import datetime

import aiohttp
import requests


async def producer_json(data, url):
    msg = ""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json={"data": data}) as response:
                if response.status == 200:
                    response_data = await response.json()  # Await the JSON response
                    msg = f"Sent Data at {datetime.now()} to {url}, App Returned: {response_data}"
                else:
                    msg = f"Failed to send - {await response.text()}"

        except Exception as e:
            msg = f"Error Sending data to Application {url}: {e}"

        finally:
            return msg


# async def producer_json(data, url):
#     msg = ""
#     try:
#         response = requests.post(url, json={'data': data})
#
#         if response.status_code == 200:
#             msg = f"Sent Data at {datetime.now()} to {url}, App Returned: {response.json()}"
#         else:
#             msg = f"Failed to send - {response.text}"
#
#     except Exception as e:
#         msg = f"Error Sending data to Application {url}: {e}"
#
#     finally:
#         return msg


def producer_json_nparray(data, url):
    msg = ""
    try:
        payload = data.tolist()
        response = requests.post(url, json={"data": payload})

        if response.status_code == 200:
            msg = f"Sent Data at {datetime.now()} to {url}, App Returned: {response.json()}"
        else:
            msg = f"Failed to send - {response.text} to {url} at {datetime.now()}"

    except Exception as e:
        msg = f"Error Sending data to Application to {url}: {e}"

    finally:
        return msg


def producer_gzipped(data, url):

    with gzip.open("data.json.gz", "wt") as f:
        f.write(data.to_json())

    # Confirm the file is gzipped
    with open("data.json.gz", "rb") as f:
        print(f.read(2))  # Should output: b'\x1f\x8b' (gzip magic number)

    # Open the compressed file in binary mode and send it as a file
    with open("data.json.gz", "rb") as f:
        files = {"file": ("data.json.gz", f, "application/gzip")}
        response = requests.post(url, files=files)

    if response.status_code == 200:
        result = response.json().get("result")
        print(f"Sent file at {datetime.now()}, {url} returned: {result}")
    else:
        print(f"Failed to send - {response.text} to {url} at {datetime.now()}")

    del data
    os.remove("data.json.gz")
