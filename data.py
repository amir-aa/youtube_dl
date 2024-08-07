from dataclasses import *
import logging,queue
import downloader,threading
import ffmpeg,resource
from concurrent.futures import ThreadPoolExecutor

proxies = {
   'http': 'http://172.16.6.6:8200',
   'https': 'http://172.16.6.6:8200',
}


def set_limits(cputime:int=60,memory:int=1):
    cpu_time_limit=cputime  # seconds
    resource.setrlimit(resource.RLIMIT_CPU,(cpu_time_limit,cpu_time_limit))
    memory_limit = memory * 1024 * 1024 * 1024  #bytes = 1GB
    resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))

def merge_audio_video(video_file, audio_file, output_file):
        import os,secrets
        if os.path.isfile(output_file):
            output_file=output_file[:-4]+secrets.token_hex(4)+".mp4" # make a random name
        video_stream = ffmpeg.input(video_file)
        audio_stream = ffmpeg.input(audio_file)

    
        ffmpeg.output(video_stream,audio_stream, output_file,vcodec='copy',acodec='aac', strict='experimental').run()

    
def start_merge(tasks:list):
    """tasks = [("a.mp4", "a.mp3", "o.mp4"),] | tasks should be a list of tuples"""

    with ThreadPoolExecutor(max_workers=4) as executor:  # Maximum workers Process
        futures = [executor.submit(merge_audio_video, video, audio, output) for video, audio, output in tasks]
        for future in futures:
            try:
                future.result()  # Wait for all futures to complete

            except Exception as e:
                logging.error("ERROR on merging")
                print(f"An error occurred: {e}")

        
@dataclass
class AudioVideo:
    videoid=str
    videoname:str
    size:int #MB
    download_link:str
    length:int #milisec
    has_audio:bool
    audioLink:str
    quality:str
  

    def download_video(self,new_thread=True):
        new_name=f"{str(self.videoid)}_{self.quality}.mp4"
        if(new_thread):
            t=threading.Thread(target=downloader.download_file(self.download_link,new_name))
            t.start()
            print(f"downloading {new_name}.mp4 started in a new thread")
            logging.info(f"downloading {new_name}.mp4 started in a new thread")
            t.join()
        else:
            downloader.download_file(self.download_link,new_name)
            logging.info(f"downloading {new_name} started")


    def download_audio(self):
        t=threading.Thread(downloader.download_file(self.audioLink,f"{self.videoid}_{self.quality}.mp3"))
        t.start()
        t.join()
        

    def Merge_audio_video(self,videofile,audiofile):
        from hashlib import md5
        _tohash=f"{self.videoid}_{self.quality}"
        self.merged_name=md5(_tohash.encode()).hexdigest()+".mp4"
        start_merge([(videofile,audiofile,self.merged_name,)])
        print(f"Video {str(self.videoid)} in process")
        return self.merged_name
    def Download_merge(self):
        self.download_video()
        self.download_audio()
        return self.Merge_audio_video(f"{str(self.videoid)}_{self.quality}.mp4",f"{str(self.videoid)}_{self.quality}.mp3")
def get_data_rapidAPI(videoID:str):
    import requests

    url = "https://youtube-media-downloader.p.rapidapi.com/v2/video/details"

    querystring = {"videoId":videoID}

    headers = {
        "x-rapidapi-key": "KEY",
        "x-rapidapi-host": "RAPID"
    }
    
    #response = requests.get(url, headers=headers, params=querystring,proxies=proxies)
    response = requests.get(url, headers=headers, params=querystring)
    return response.json()


def Serialize_response_by_quality(response)-> dict:
    items=response["videos"]["items"]
    result={}
    
    i=0
    for video in items:
        try:
            audio=response["audios"]["items"][i]["url"]
            _instance=AudioVideo(response["title"],video["size"],video["url"],video["lengthMs"],video["hasAudio"],audio,video["quality"])
            _instance.videoid=response["id"]
      
            result[str(_instance.quality)]=_instance
            i+=1
        except Exception as ex:
                logging.error("error on link "+str(ex))
            #audio=""
        
    
    return result



def main():
    data=get_data_rapidAPI("2hEoV3qeNmI")
    sdata=Serialize_response_by_quality(data,False)
    for item in sdata:

        item.Download_merge()
#main()
