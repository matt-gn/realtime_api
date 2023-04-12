from urllib.request import urlopen
from urllib.error import HTTPError
from datetime import date, timedelta
from PIL import Image
import requests
from io import BytesIO

def harvest_gif_images() -> list:
    today = date.today().strftime("%Y/%m%d")                                    ## Get today and yesterday's date
    yesterday = (date.today() - timedelta(days = 1)).strftime("%Y/%m%d")
    two_days_ago = (date.today() - timedelta(days = 2)).strftime("%Y/%m%d")
    spec_channels = (                                                           ## Spec channels + their location
        ('Infrared', 'sat4kminfraredgif'),                                      ## on our THREDDS server
        ('Longwave', 'sat4kmlongwavegif'),
        ('Shortwave', 'sat4kmshortwavegif'),
        ('Visible', 'sat4kmvisiblegif'),
        ('Water Vapor', 'sat4kmwatervaporgif')
    )
    urls = []                                                                   ## Container to store channel image urls
    def get_image_url(url: str):
        try:
            image_catalog = (urlopen(url)                                       ## Access the catalog XML file and read
                    .read()
                    .decode('utf-8'))
            image_catalog = image_catalog.split('\n')                           ## Parse the data
            image_catalog = [line.strip() for line in image_catalog             ## Locate the urls for the small .jpgs
                             if 'urlPath' in line and 'small' in line]
            image_catalog = [word.split('"')[1]                                 ## Extract just the file extension
                             for line in image_catalog 
                             for word in line.split() if 'urlPath' in word]
            ##latest_image = image_catalog[-1]
            url = 'https://amrdcdata.ssec.wisc.edu/thredds/fileServer/'         ## This is the THREDDS file server host URL
            return [f'{url}{image}' for image in image_catalog]                 ## Return the formatted string
        except HTTPError:
            return []
    for (key, value) in spec_channels:                                          ## Do this for each channel
        channel_urls = []
        two_days_ago_images = get_image_url(
            f'https://amrdcdata.ssec.wisc.edu/thredds/catalog/{value}/{two_days_ago}/catalog.xml'
            )
        channel_urls += two_days_ago_images
        yesterday_images = get_image_url(                                       ## Using yesterday's date,
            f'https://amrdcdata.ssec.wisc.edu/thredds/catalog/{value}/{yesterday}/catalog.xml'
            )
        channel_urls += yesterday_images
        latest_images = get_image_url(                                          ## And today's date.
            f'https://amrdcdata.ssec.wisc.edu/thredds/catalog/{value}/{today}/catalog.xml'
            )
        channel_urls += latest_images                                           ## We add them to the end of the channel url container
        urls.append((key, channel_urls[-12:]))                                  ## Then append the most recent 12 to our list
    return urls

def make_gif(sat_images):                                                       ## This builds a gif from a url
    def get_web_image(channel, url):                                            ## Helper method to retrieve image
        response = requests.get(url)
        img = Image.open(BytesIO(response.content))
        # Resize
        if channel == "Water Vapor":                                            ## Quantize colors for compression: 32 bit for WV,
            img = img.quantize(colors=32)
        else:
            img = img.quantize(colors=16)                                       ## 16 bit for all others (primarily grayscale)
        img = img.resize((350, 350), resample=Image.LANCZOS)
        out = BytesIO()
        img.save(out, 'GIF')
        out.seek(0)
        return out                                                              ## Return the resized and compressed image
    for channel, urls in sat_images:                                            ## For channel url list,
        try:
            images = [Image.open(get_web_image(channel, url)) for url in urls]      ## Process each linked image
            file_out = f"/usr/local/realtime_api/static/{channel}.gif"
            # Save the animated GIF
            images[0].save(file_out, save_all=True, append_images=images[1:],       ## Save as animated gif in the 'gifs' folder
                        duration=300, loop=0, optimize=True)
        except:
            print("Error processing gif: " + channel)

if __name__ == "__main__":
    urls = harvest_gif_images()
    make_gif(urls)

