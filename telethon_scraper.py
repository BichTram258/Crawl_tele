import sys
import json
import os
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import random
import base64
from datetime import datetime, timedelta
import argparse

re="\033[1;31m"
gr="\033[1;32m"
cy="\033[1;36m"

parser = argparse.ArgumentParser(description='Script description')
parser.add_argument('--api_id', type=int, help='Telegram API ID')
parser.add_argument('--api_hash', type=str, help='Telegram API hash')
parser.add_argument('--phone', type=str, help='Phone number')
parser.add_argument('--group_name', type=str, help='Telegram group name')
parser.add_argument('--action', type=str, help='Scrape action')
parser.add_argument('--options', type=str, help='Scrape options')
args = parser.parse_args()

api_id = args.api_id or 'YOUR_API_ID' #'YOUR_API_ID'
api_hash = args.api_hash or 'YOUR_API_HASH' #'YOUR_API_HASH'
phone = args.phone or '+84xxxxxxxxx'#'YOUR_PHONE_NUMBER'
group_name = args.group_name
action = args.action
options = args.options or '{}' #'{"offset_date": 0, "limit": 3}'

client = TelegramClient(phone, api_id, api_hash)
client.connect()
if not client.is_user_authorized():
    client.send_code_request(phone)
    os.system('clear')
    client.sign_in(phone, input(gr+'[+] Enter the code: '+re))

def convert_bytes_or_datetime_to_strings(data):
    if isinstance(data, bytes):
        return base64.b64encode(data).decode('utf-8')
    elif isinstance(data, datetime):
        return data.isoformat()
    elif isinstance(data, dict):
        return {convert_bytes_or_datetime_to_strings(key): convert_bytes_or_datetime_to_strings(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_bytes_or_datetime_to_strings(item) for item in data]
    else:
        return data

async def get_messages():

    options_dict = json.loads(options)
    offset_date = options_dict.get('offset_date', None)
    limit = options_dict.get('limit', 100)
    if offset_date:
        offset_date = datetime.now() - timedelta(days=offset_date)
    else:
        offset_date = None

    messages = []
    async for message in client.iter_messages(group_name, limit=limit, offset_date=offset_date):
        messages.append(message)

    return messages

async def main():
    if group_name is None:
        print('Error: --group_name parameter is required e.g "jobremotevn"')
        sys.exit(1)
    if action is None:
        print('Error: --action parameter is required e.g "scrape_members" or "scrape_messages"')
        sys.exit(1)

    group = await client.get_entity(group_name)

    if action == 'scrape_members':
        members = await client.get_participants(group, aggressive=True)
        members_json = [m.to_dict() for m in members]
        members_json = convert_bytes_or_datetime_to_strings(members_json)
        json_str = json.dumps(members_json, ensure_ascii=False)
        print(json_str)

        file_name = f"{action}-{group_name}.json"

        if not os.path.exists("data"):
            os.makedirs("data")
        with open(os.path.join("data", file_name), "w") as f:
            json.dump(json.loads(json_str), f)

    elif action == 'scrape_messages':
        messages = await get_messages()
        messages_json = [m.to_dict() for m in messages]
        messages_json = convert_bytes_or_datetime_to_strings(messages_json)
        json_str = json.dumps(messages_json, ensure_ascii=False)
        print(json_str)

        file_name = f"{action}-{group_name}-{options}.json"

        if not os.path.exists("data"):
            os.makedirs("data")

        with open(os.path.join("data", file_name), "w") as f:
            json.dump(json.loads(json_str), f)

    else:
        raise ValueError(f'Invalid action: {action}. Only "scrape_members" and "scrape_messages" are supported.')

    await client.disconnect()

with client:
    if client.is_user_authorized():
        client.loop.run_until_complete(main())
