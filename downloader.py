import requests
def download_file(url, destination):

    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status() 
            with open(destination, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:  
                        file.write(chunk)
        print(f"Download completed and saved to {destination}")
    except requests.RequestException as e:
        print(f"Error during download: {e}")
