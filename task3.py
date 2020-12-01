import collections
import random
import sys
import tweepy

def check_ascii(input_str):

    try:
        input_str.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True


class Listener(tweepy.StreamListener):
    
    def __init__(self, file_name):
        super(Listener, self).__init__()
        self.file_name = file_name
        self.count = 0
        self.hash_100_list = []
        self.total = 100
    
    
    def on_status(self, status):
    
        tags = status.entities['hashtags']
        
        if len(tags) > 0:
        
            tags_text = [val['text'] for val in tags]
            tags_text = [val.replace('\n', ' ') for val in tags_text]
            
            english_text = []
            
            for val in tags_text:
                if check_ascii(val):
                    english_text.append(val)
                    
            if len(english_text) > 0:
            
                self.count += 1
                if self.count <= 100:
                    self.hash_100_list.append(english_text)
                else:
                    probability = random.randint(0, (self.count-1))
                    
                    if probability < self.total:
                    
                        self.hash_100_list.pop(probability)
                        self.hash_100_list.append(english_text)
                        
                combined_list = []
                for small_lis in self.hash_100_list:
                    for val in small_lis:
                        combined_list.append(val)
                        
                combined_dict = dict(collections.Counter(combined_list))
                sorted_counts = sorted(list(set(combined_dict.values())), reverse=True)
                top_3_counts = sorted_counts[:min(3, len(sorted_counts))]
                
                items_to_print = []
                
                for val,cnt in combined_dict.items():
                    if cnt in top_3_counts:
                        items_to_print.append((val, cnt))
                    
                items_to_print = sorted(items_to_print, key = lambda x:(-x[1],x[0]))
                
                fp = open(self.file_name, "a")
                fp.write("The number of tweets with tags from the beginning: "+str(self.count)+"\n")
                
                for everyV in items_to_print:
                    fp.write(everyV[0] + ":" + str(everyV[1]) + "\n")
                fp.write("\n")
                
                fp.close()
                
    
    def on_error(self, status_code):
    
        if status_code == 420:
            return False


if __name__ == "__main__":

    port = int(sys.argv[1])
    output_file = sys.argv[2]
    
    # Just doing this create the file newly and erase the old content
    with open(output_file, "w") as fp:
        fp.write("")
    
    con_key = "EWivf8c2N9PnyEd7APSoW8oOW"
    con_secret = "j1Z9Cxz2HFBaw8zMOSBQT3GmlmYObdc9QNVL7cjJdXcRdktc0v"
    
    accessToken = "1321853523059904512-Oy2xahli1eSwRjtTZIt3PikAmvPeHw"
    accessTokenSecret = "Hxh0BPq2SDiJjeiqtNkSRNWwXaceN5ajBPvhQ0F4tazoq"
    
    auth = tweepy.OAuthHandler(con_key, con_secret)
    auth.set_access_token(accessToken, accessTokenSecret)
    
    api = tweepy.API(auth)
    data = tweepy.Stream(auth = api.auth, listener = Listener(output_file))
    
    track_items = ['usa', 'election', 'trump', 'covid', 'pandemic', 'biden', 'india', 'diwali']
    
    data.filter(track=track_items, languages=['en'])
    