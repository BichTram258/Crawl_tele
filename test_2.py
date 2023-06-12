import sys
import json
import os
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import ChannelParticipantsAdmins
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
from telethon.errors import UsernameInvalidError
from telethon import errors
import random
import base64
from datetime import datetime, timedelta, date
import time
import argparse
import pymongo

def groupname():
    client_db = pymongo.MongoClient("mongodb+srv://bitscope:7EUw7YgsvBKCw7Qc@bitscopedb.oeifxkf.mongodb.net/?retryWrites=true&w=majority")
    db = client_db["test"]
    collection = db["telegrams"]
    while True:
        query = {"$and": [
            {"_id": {"$exists": True}},
            {"status": "0"},
            {"data.status": "0"}
        ]}
        record = collection.find_one(query)
        if not record:
                time.sleep(2)
                continue
        group_name = record["profile"]["name"]
        if not group_name:
                time.sleep(2)
                continue
        return group_name
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
api_id = args.api_id or '25191559'
api_hash = args.api_hash or '69c8545670bab0234280eb8dcabeee15'
phone = args.phone or '+84373597908'
group_name = f'{groupname()}'
action = args.action or 'scrape_members_admins_messages'
options = args.options or '{"offset_date": 16, "limit": 2000}'
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

async def get_all_members(group_name):
    queryKey = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    all_members = []
    all_member_ids = []
    channel = group_name
    for key in queryKey:
        async for member in client.iter_participants(group_name, search=key, aggressive=True):
            if member.id not in all_member_ids:
                    all_member_ids.append(member.id)
                    all_members.append(member)
    return all_members 

async def main():
    while True:
        client_db = pymongo.MongoClient("mongodb+srv://sontng:So0373597908@cluster0.ms4zoal.mongodb.net/test")
        db = client_db["test"]
        collection = db["telegrams"]
        query = {"$and": [
            {"_id": {"$exists": True}},
            {"status": "0"},
            {"data.status": "0"}
        ]}
        record = collection.find_one(query)
        if not record:
            time.sleep(3)
            continue
        group_name = record["profile"]["name"]
        if not group_name:
            time.sleep(3)
            continue
        try:
            group = await client.get_entity(group_name)
            if action == 'scrape_members':
                members_count = (await client.get_participants(group, limit=0)).total
                if members_count > 10000:
                    normal_search_members = await client.get_participants(group, aggressive=True)
                    alphabet_search_members = await get_all_members(group_name)
                    merged_members = normal_search_members + alphabet_search_members
                    unique_ids = {member.id for member in merged_members}
                    unique_members = [member for member in merged_members if member.id in unique_ids]
                    members = unique_members
                else:
                    members = await client.get_participants(group, aggressive=True)
                members_json = [m.to_dict() for m in members]
                members_json = convert_bytes_or_datetime_to_strings(members_json)
                json_str = json.dumps(members_json, ensure_ascii=False)
                file_group_name = f"{action}-{group_name}.json"
                if not os.path.exists("data"):
                    os.makedirs("data")
                with open(os.path.join("data", file_group_name), "w") as f:
                    json.dump(json.loads(json_str), f)
            elif action == 'scrape_admins':
                admins = await client.get_participants(group, aggressive=True, filter=ChannelParticipantsAdmins)
                admins_json = [a.to_dict() for a in admins]
                admins_json = convert_bytes_or_datetime_to_strings(admins_json)
                json_str = json.dumps(admins_json, ensure_ascii=False)
                file_group_name = f"{action}-{group_name}-{options}.json"
                if not os.path.exists("data"):
                    os.makedirs("data")
                with open(os.path.join("data", file_group_name), "w") as f:
                    json.dump(json.loads(json_str), f)
            elif action == 'scrape_messages':
                messages = await get_messages()
                messages_json = [m.to_dict() for m in messages]
                messages_json = convert_bytes_or_datetime_to_strings(messages_json)
                json_str = json.dumps(messages_json, ensure_ascii=False)
                file_group_name = f"{action}-{group_name}-{options}.json"
                if not os.path.exists("data"):
                    os.makedirs("data")
                with open(os.path.join("data", file_group_name), "w") as f:
                    json.dump(json.loads(json_str), f)
            elif action == 'scrape_members_admins_messages':
                # if group.photo:
                #     photo = group.photo
                #     # dc_id = photo.dc_id
                #     result = await client.download_profile_photo(photo)

                #     # avatar_url = f"https://cdn{dc_id}.telegram-cdn.org/file/{access_hash}/{file_reference}.jpg"
                #     collection.update_one({"_id": record["_id"]}, 
                #                     {"$set": {"overview.avatar": result}})
                members_count = (await client.get_participants(group, limit=0)).total
                collection.update_one({"_id": record["_id"]}, 
                                      {"$set": {"data.totalMembers": members_count}})
                admins = await client.get_participants(group, aggressive=True, filter=ChannelParticipantsAdmins)
                if members_count > 10000:
                    normal_search_members = await client.get_participants(group, aggressive=True)
                    alphabet_search_members = await get_all_members(group_name)
                    merged_members = normal_search_members + alphabet_search_members
                    unique_ids = {member.id for member in merged_members}
                    unique_members = [member for member in merged_members if member.id in unique_ids]
                    members = unique_members
                else:
                    members = await client.get_participants(group, aggressive=True)
                options_dict = json.loads(options)
                offset_date = options_dict.get('offset_date', None)
                limit = options_dict.get('limit', 50)
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
                    batch_size = 50
                    while members or admins or messages:
                        if members:
                            batch_members = members[:batch_size]
                            process_members = await process_and_print_members(batch_members)
                            members = members[batch_size:]
                            if process_members:
                                collection.update_one(
                                    {"_id": record["_id"]},
                                    {'$push': {
                                        'data.dataMembers': {
                                            '$each': json.loads(process_members)
                                        }
                                    }}
                                )
                        if admins:
                            batch_admins = admins[:batch_size]
                            process_admins = await process_and_print_admins(batch_admins)
                            admins = admins[batch_size:]
                            if process_admins:
                                collection.update_one(
                                    {"_id": record["_id"]},
                                    {'$push': {
                                        'data.dataAdmins': {
                                            '$each': json.loads(process_admins)
                                        }
                                    }}
                                )
                        if messages:
                            batch_messages = messages[:batch_size]
                            process_messages = await process_and_print_messages(batch_messages)
                            messages = messages[batch_size:]
                            if process_messages:
                                collection.update_one(
                                    {"_id": record["_id"]},
                                    {'$push': {
                                        'data.dataMessages': {
                                            '$each': json.loads(process_messages)
                                        }
                                    }}
                            )
                        collection.update_one({"_id": record["_id"]}, {"$set": {'status': "1", 'data.status': "1"}}) 
                collection.update_one({"_id": record["_id"]}, {"$set": {'status': "2", 'data.status': "2"}})   
                time.sleep(random.randint(1, 3))  
                continue
            elif action == 'scrape_all_members':
                members = await get_all_members()
                members_json = [m.to_dict() for m in members]
                members_json = convert_bytes_or_datetime_to_strings(members_json)
                json_str = json.dumps(members_json, ensure_ascii=False)
                file_group_name = f"{action}-{group_name}-{options}.json"
                if not os.path.exists("data"):
                    os.makedirs("data")
                with open(os.path.join("data", file_group_name), "w") as f:
                    json.dump(json.loads(json_str), f)
            elif action == 'scrape_members_count':
                members_count = (await client.get_participants(group, limit=0)).total
                json_str = json.dumps({ "members_count": members_count })
            else:
                raise ValueError(f'Invalid action: {action}. Only "scrape_members" , "scrape_messages" and "scrape_members_count" are supported.')
        except ValueError:
            collection.update_one({"_id": record["_id"]}, {"$set": {'status': "4", 'data.status': "4"}}) 
            continue
        except UsernameInvalidError:
            collection.update_one({"_id": record["_id"]}, {"$set": {'status': "4", 'data.status': "4"}}) 
            continue
        except errors.ChatAdminRequiredError:
            collection.update_one({"_id": record["_id"]}, {"$set": {'status': "4", 'data.status': "4", "overview.percent": 0}}) 
            continue

with client:
    if client.is_user_authorized():
        client.loop.run_until_complete(main())