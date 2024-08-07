import requests,secrets,base64
import json,os,asyncio,hypercorn
import ffmpeg,asyncio
from hashlib import md5
from concurrent.futures import ThreadPoolExecutor
from flask import Flask,jsonify
import data,datetime,logging
_executor = ThreadPoolExecutor(max_workers=4)
app=Flask(__name__)
BUFFER_LIFETIME=120 #sec
def is_expired(input:datetime)->bool:
    dif=datetime.datetime.now() - input
    print(dif)
    if dif > datetime.timedelta(seconds=120):
        return True
    return False


video_buffer={}
@app.route('/getdata/<vid>')
def index_get_data(vid:str):

        #video_data=[obj_dict.__dict__ for obj_dict in video_objects.values()]
     
        #print(isinstance(v,data.AudioVideo))
        hashed=md5(vid.encode()).hexdigest()
        if hashed in video_buffer.keys() :
            if not is_expired (video_buffer[hashed]["buffered_at"]):
                print("from buffer")
                return jsonify(video_buffer[hashed])
            else:
                 print("expired buffered value")
                 del video_buffer[hashed]
        
        raw_video_data=data.get_data_rapidAPI(vid)
        video_objects=data.Serialize_response_by_quality(raw_video_data)
        video_buffer[hashed]=video_objects
        video_buffer[hashed]["buffered_at"]=datetime.datetime.now()
        return jsonify(video_objects)
@app.route('/getdata_single/<vid>/<quality>')
def get_data(vid,quality):

    hashed=md5(vid.encode()).hexdigest()
    try:
        if hashed in video_buffer.keys():
            return jsonify(video_buffer[hashed][quality])
        else:
            index_get_data(vid)
            return jsonify(video_buffer[hashed][quality])
        
    except KeyError:
         return jsonify({"type":"error", "message":"Quality or video id does not exists"}),404
    except Exception as ex:
         return jsonify({"type":"error", "message":str(ex)}),500


async def run_tasks_async(vobject:data.AudioVideo):
    loop = asyncio.get_event_loop()
    futures = [
        loop.run_in_executor(_executor, vobject.Download_merge,)
        
    ]
    results = []
    for future in futures:
        try:
            result = await future
            results.append(result)
        except Exception as e:
            logging.error("ERROR on merging")
            results.append(f"An error occurred: {e}")
    return results




@app.route('/action/<vid>/<quality>')
async def get_merge_video(vid,quality):
    hashed=md5(vid.encode()).hexdigest()
    try:
        if hashed in video_buffer.keys():
            _obj=video_buffer[hashed][quality]
        else:
            index_get_data(vid)
            _obj=video_buffer[hashed][quality]
        r= await run_tasks_async(_obj)
        print(r)
        return jsonify(r)
    except KeyError:
         return jsonify({"type":"error", "message":"Quality or video id does not exists"}),404
    except Exception as ex:
         return jsonify({"type":"error", "message":str(ex)}),500
     
