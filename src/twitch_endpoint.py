import requests
import os
import sys
import time
import math
import json
import random


MAIN_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(MAIN_DIR, "data/")
sys.path.append(MAIN_DIR)


class TwitchEndpoint():
    def __init__(self, credentials_path):
        self.credentials_path = credentials_path
        self.creds_cache = self._load_credentials_cache()
        self.current_token_index = 0
        self.data = []


    def _load_credentials_cache(self):
        # Load rate limits from cache file
        with open(self.credentials_path, "r") as f:
            cred_cache = json.load(f)
            cred_cache = cred_cache["twitch_credentials"]
        return cred_cache


    def _cache_creds(self):
        # Save rate limits to cache file
        with open(self.credentials_path, "w") as f:
            json.dump({"twitch_credentials": self.creds_cache}, f)
        
        
    def get_tokens(self):
        for idx, cred in enumerate(self.creds_cache):
            time.sleep(1)
            current_time = time.time()
            if cred.get("current_token") == "" or cred.get("token_expiry") <= current_time:
                current_token = cred.get("current_token")
                print(f"Getting tokens for: {current_token}")       
                body = {
                    "client_id": cred["client_id"],
                    "client_secret": cred["client_secret"],
                    "grant_type": "client_credentials"
                }
                res = requests.post('https://id.twitch.tv/oauth2/token', body)
                res_data = res.json()
                self.creds_cache[idx]["current_token"] = res_data["access_token"]
                self.creds_cache[idx]["token_expiry"] = current_time + 5214015
                self._cache_creds()

    
    def read_selected(self):
        selected_ids_path = DATA_DIR + "ids/user_ids"
        with open(selected_ids_path, "r") as f:
            user_ids = []
            lines = f.readlines()
            user_ids = [x.strip() for x in lines if x.strip() != ""]
            
        return user_ids
        
    
    def read_scraped(self, filename):
        scraped_path = DATA_DIR + "/scraped/" + filename
        if not os.path.isfile(scraped_path):
            return None
        else:
            scraped_data = []
            with open(scraped_path, "r") as f:
                lines = f.readlines()
                for line in lines:
                    scraped_data.append(json.loads(line))
                
            return scraped_data


    def dedup_ids(self, selected_ids, scraped_data, filename, id_key):
        scraped_ids = set([user_info.get(id_key) for user_info in scraped_data])
        
        return list(scraped_ids.symmetric_difference(set(selected_ids)))
    
    
    def _get_user_info_batches(self, user_info_ids, batch_size):
        batches = []
        batches_no = math.ceil(len(user_info_ids) / batch_size)
        starting_batch_idx = 0
        for batch_idx in range(0, batches_no):
            ending_batch_idx = starting_batch_idx + batch_size
            batches.append(user_info_ids[starting_batch_idx:ending_batch_idx])
            starting_batch_idx = starting_batch_idx + batch_size
        
        return batches

    
    def _choose_creds(self):
        available_tokens_idx = []
        ratelimit_reset_min = 0
        ratelimit_reset_min_idx = 0
        for idx, cred in enumerate(self.creds_cache):
            if cred.get("ratelimit_reset") <= ratelimit_reset_min:
                ratelimit_reset_min =  cred.get("ratelimit_reset")
                ratelimit_reset_min_idx = idx
            if cred.get("ratelimit_remaining") > 0:
                available_tokens_idx.append(idx)
        if len(available_tokens_idx) > 0:
            self.current_token_index = available_tokens_idx[0]
        else:
            current_time = time.time()
            time.sleep(ratelimit_reset_min - current_time)
            self.current_token_index = ratelimit_reset_min_idx
            
        selected_token = self.creds_cache[self.current_token_index].get("current_token")
        selected_client = self.creds_cache[self.current_token_index].get("client_id")
        headers = {
            "Authorization": f"Bearer {selected_token}",
            "Client-Id": selected_client
        }
        
        return headers
    
    
    def _get_request(self, url):
        print(f"Collecting from endpoint: {url}\n")
        self.creds_cache = self._load_credentials_cache()
        headers = self._choose_creds()
        res = requests.get(url, headers=headers)
        data_collected = json.loads(res.text)["data"]
        self.creds_cache[self.current_token_index]["ratelimit_limit"] = int(res.headers.get("Ratelimit-Limit"))
        self.creds_cache[self.current_token_index]["ratelimit_remaining"] = int(res.headers.get("Ratelimit-Remaining"))
        self.creds_cache[self.current_token_index]["ratelimit_reset"] = int(res.headers.get("Ratelimit-Reset"))        
        self._cache_creds()
        
        return data_collected


    def _get_request_pagination(self, url):
        url_base = url + "&first=100" 
        data_collected = []
        
        # Do first run then append data
        print(f"Collecting from endpoint: {url_base}\n")
        self.creds_cache = self._load_credentials_cache()
        headers = self._choose_creds()
        res = requests.get(url_base, headers=headers)
        pagination = json.loads(res.text).get("pagination")
        data_collected.extend(json.loads(res.text).get("data"))
        self.creds_cache[self.current_token_index]["ratelimit_limit"] = int(res.headers.get("Ratelimit-Limit"))
        self.creds_cache[self.current_token_index]["ratelimit_remaining"] = int(res.headers.get("Ratelimit-Remaining"))
        self.creds_cache[self.current_token_index]["ratelimit_reset"] = int(res.headers.get("Ratelimit-Reset"))        
        self._cache_creds()
        
        # While there is pagination, continue with the request 
        while pagination:
            curser = pagination.get("cursor")
            url_new = url_base + f"&after={curser}"
            print(f"Collecting from endpoint: {url_new}\n")
            self.creds_cache = self._load_credentials_cache()
            headers = self._choose_creds()
            res = requests.get(url_new, headers=headers)
            pagination = json.loads(res.text).get("pagination")
            data_collected.extend(json.loads(res.text).get("data"))
            self.creds_cache[self.current_token_index]["ratelimit_limit"] = int(res.headers.get("Ratelimit-Limit"))
            self.creds_cache[self.current_token_index]["ratelimit_remaining"] = int(res.headers.get("Ratelimit-Remaining"))
            self.creds_cache[self.current_token_index]["ratelimit_reset"] = int(res.headers.get("Ratelimit-Reset"))
    
        return data_collected
    
    
    def _save_collected_data(self, data_collected, filename):
        scraped_path = DATA_DIR + "/scraped/" + filename
        with open(scraped_path, "a") as f:
            for data in data_collected:
                f.write(json.dumps(data) + "\n")
        
    
    def collect_save_user_info(self, user_ids, batch_size):
        print(f"Collecting user info data for {len(user_ids)} users...")
        batches = self._get_user_info_batches(user_ids, batch_size)
        for idx, user_ids in enumerate(batches):
            if idx+1 % 10 == 0:
                time.sleep(random.randint(2,5))
            request_user_ids = ["id="+id for id in user_ids]
            request_user_ids = "&".join(request_user_ids)
            url = 'https://api.twitch.tv/helix/users?' + request_user_ids
            data_collected = self._get_request(url)
            if len(data_collected) > 0:
                self._save_collected_data(data_collected, "user_info")
            

    def collect_save_user_channel(self, user_ids, batch_size):
        print(f"Collecting user channel data for {len(user_ids)} users...")
        batches = self._get_user_info_batches(user_ids, batch_size)
        for idx, user_ids in enumerate(batches):
            if idx+1 % 10 == 0:
                time.sleep(random.randint(2,5))
            request_user_ids = ["broadcaster_id="+id for id in user_ids]
            request_user_ids = "&".join(request_user_ids)
            url = 'https://api.twitch.tv/helix/channels?' + request_user_ids
            data_collected = self._get_request(url)
            self._save_collected_data(data_collected, "user_channel")
            if len(data_collected) > 0:
                self._save_collected_data(data_collected, "user_channel")


    def collect_save_user_video(self, user_ids, batch_size):
        print(f"Collecting user video data for {len(user_ids)} users...")
        batches = self._get_user_info_batches(user_ids, batch_size)
        for idx, user_ids in enumerate(batches):
            if idx+1 % 10 == 0:
                time.sleep(random.randint(2,5))
            request_user_ids = ["user_id="+id for id in user_ids]
            request_user_ids = "&".join(request_user_ids)
            url = 'https://api.twitch.tv/helix/videos?' + request_user_ids
            data_collected = self._get_request_pagination(url)
            self._save_collected_data(data_collected, "user_video")
            if len(data_collected) > 0:
                self._save_collected_data(data_collected, "user_video")
    

if __name__ == "__main__":
    twitch = TwitchEndpoint(MAIN_DIR + "/credentials_ratelimit_cache.json")
    twitch.get_tokens()
    
    # # Get user information
    # selected_ids = twitch.read_selected()
    # scraped_data = twitch.read_scraped("user_info")
    # if scraped_data:
    #     user_info_ids = twitch.dedup_ids(selected_ids, scraped_data, "user_info", "id")
    # else:
    #     user_info_ids = selected_ids

    # twitch.collect_save_user_info(user_info_ids, batch_size=100)
    # print("Collected all user info\n\n")
    
    # # Get channel information
    # selected_ids = twitch.read_selected()
    # scraped_data = twitch.read_scraped("user_channel")
    # if scraped_data:
    #     user_channel_ids = twitch.dedup_ids(selected_ids, scraped_data, "user_channel", "broadcaster_id")
    # else:
    #     user_channel_ids = selected_ids

    # twitch.collect_save_user_channel(user_channel_ids, batch_size=100)
    # print("Collected all user channels\n\n")
    
    # Get user videos
    selected_ids = twitch.read_selected()
    scraped_data = twitch.read_scraped("user_video")
    if scraped_data:
        user_video_ids = twitch.dedup_ids(selected_ids, scraped_data, "user_video", "user_id")
    else:
        user_video_ids = selected_ids

    twitch.collect_save_user_video(user_video_ids, batch_size=1)
    print("Collected all user videos\n\n")
    
    # Update user followers? Who are following them?
    
    
