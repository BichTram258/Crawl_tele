import sys
import json
import os
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import ChannelParticipantsAdmins
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
import random
import base64
from datetime import datetime, timedelta, date
import argparse
import pymongo
client_db = pymongo.MongoClient("mongodb+srv://sontng:So0373597908@cluster0.ms4zoal.mongodb.net/test")
db = client_db["test"]
collection_requests = db["requests"]
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

api_id = args.api_id or '25191559' #'YOUR_API_ID'
api_hash = args.api_hash or '69c8545670bab0234280eb8dcabeee15' #'YOUR_API_HASH'
phone = args.phone or '+84373597908'#'YOUR_PHONE_NUMBER'
group_name = args.group_name or "mcan_coin"
action = args.action or 'scrape_members_admins_messages'
options = args.options or '{"offset_date": 16, "limit": 2000}' #'{"offset_date": 0, "limit": 3}'

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

def datetime_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()

async def get_messages():
    options_dict = json.loads(options)
    offset_date = options_dict.get('offset_date', None)
    limit = options_dict.get('limit', 2000)
    if offset_date:
        offset_date = date.today() - timedelta(days=offset_date-1)
    else:
        offset_date = None

    messages = []
    min_id = 0
    max_id=0
    _continue = True
    async for message in client.iter_messages(group_name, limit=1, offset_date=offset_date):
        min_id = message.id
    while _continue :
        _messages=[]
        if(max_id==0):
            async for message in client.iter_messages(group_name, limit=limit, offset_date=0, min_id=min_id ):
                _messages.append(message)
        else:
            async for message in client.iter_messages(group_name, limit=limit, offset_date=0, min_id=min_id,max_id=max_id ):
                _messages.append(message)
        if(len(_messages) > 0):
            max_id = _messages[-1].id
        _continue = len(_messages) == limit
        messages.extend(_messages)

    return messages


async def process_and_print_messages(messages):
    messages_json = [m.to_dict() for m in messages]
    messages_json = convert_bytes_or_datetime_to_strings(messages_json)
    json_str = json.dumps(messages_json, ensure_ascii=False)
    print(json_str)
    return json_str

async def process_and_print_members(members):
    members_json = [m.to_dict() for m in members]
    members_json = convert_bytes_or_datetime_to_strings(members_json)
    json_str = json.dumps(members_json, ensure_ascii=False)
    print(json_str)
    return json_str

async def process_and_print_admins(admins):
    admins_json = [a.to_dict() for a in admins]
    admins_json = convert_bytes_or_datetime_to_strings(admins_json)
    json_str = json.dumps(admins_json, ensure_ascii=False)
    print(json_str)
    return json_str

async def get_all_members():
    queryKey = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    all_members = []
    all_member_ids = []
    channel = group_name

    for key in queryKey:
        async for member in client.iter_participants(group_name, search=key, aggressive=True):
            if member.id not in all_member_ids:
                    all_member_ids.append(member.id)
                    all_members.append(member)
    return all_members #members

async def main():
    if group_name is None:
        print('Error: --group_name parameter is required e.g "jobremotevn"')
        sys.exit(1)
    if action is None:
        print('Error: --action parameter is required e.g "scrape_members" or "scrape_messages"')
        sys.exit(1)

    group = await client.get_entity(group_name)
    
    if action == 'scrape_members':
        members_count = (await client.get_participants(group, limit=0)).total

        if members_count > 10000:
            normal_search_members = await client.get_participants(group, aggressive=True)
            alphabet_search_members = await get_all_members()

            # Merge the two lists of User objects
            merged_members = normal_search_members + alphabet_search_members
            # Remove duplicates based on the 'id' field
            unique_ids = {member.id for member in merged_members}
            # Create a list of unique User objects
            unique_members = [member for member in merged_members if member.id in unique_ids]
            members = unique_members
        else:
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

    elif action == 'scrape_admins':
        admins = await client.get_participants(group, aggressive=True, filter=ChannelParticipantsAdmins)
        admins_json = [a.to_dict() for a in admins]
        admins_json = convert_bytes_or_datetime_to_strings(admins_json)
        json_str = json.dumps(admins_json, ensure_ascii=False)
        print(json_str)

        file_name = f"{action}-{group_name}-{options}.json"

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

    elif action == 'scrape_members_admins_messages':
        members_count = (await client.get_participants(group, limit=0)).total
        admins = await client.get_participants(group, aggressive=True, filter=ChannelParticipantsAdmins)
        messages = await get_messages()

        if members_count > 10000:
            normal_search_members = await client.get_participants(group, aggressive=True)
            alphabet_search_members = await get_all_members()
            merged_members = normal_search_members + alphabet_search_members
            unique_ids = {member.id for member in merged_members}
            unique_members = [member for member in merged_members if member.id in unique_ids]
            members = unique_members
        else:
            members = await client.get_participants(group, aggressive=True)
        batch_size = 500
        file_name = f"{action}-{group_name}-{options}.json"

        while members or admins or messages:
            if members:
                batch_members = members[:batch_size]
                process_members = await process_and_print_members(batch_members)
                members = members[batch_size:]

                if not os.path.exists("data_members"):
                    os.makedirs("data_members")

                with open(os.path.join("data_members", file_name), "w") as f:
                    json.dump(json.loads(process_members), f)

            if admins:
                batch_admins = admins[:batch_size]
                process_admins = await process_and_print_admins(batch_admins)
                admins = admins[batch_size:]

                if not os.path.exists("data_admins"):
                    os.makedirs("data_admins")

                with open(os.path.join("data_admins", file_name), "w") as f:
                    json.dump(json.loads(process_admins), f)

            if messages:
                batch_messages = messages[:batch_size]
                process_messages = await process_and_print_messages(batch_messages)
                messages = messages[batch_size:]

                if not os.path.exists("data_messages"):
                    os.makedirs("data_messages")

                with open(os.path.join("data_messages", file_name), "w") as f:
                    json.dump(json.loads(process_messages), f)  
            
    elif action == 'scrape_all_members':
        members = await get_all_members()
        members_json = [m.to_dict() for m in members]
        members_json = convert_bytes_or_datetime_to_strings(members_json)
        json_str = json.dumps(members_json, ensure_ascii=False)
        print(json_str)

        file_name = f"{action}-{group_name}-{options}.json"

        if not os.path.exists("data"):
            os.makedirs("data")

        with open(os.path.join("data", file_name), "w") as f:
            json.dump(json.loads(json_str), f)
    elif action == 'scrape_members_count':
        members_count = (await client.get_participants(group, limit=0)).total
        json_str = json.dumps({ "members_count": members_count })
        print(json_str)

    else:
        raise ValueError(f'Invalid action: {action}. Only "scrape_members" , "scrape_messages" and "scrape_members_count" are supported.')

    await client.disconnect()

with client:
    if client.is_user_authorized():
        client.loop.run_until_complete(main())
