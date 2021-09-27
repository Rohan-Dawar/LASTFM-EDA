# -*- coding: utf-8 -*-
"""Rohan_Dawar_JZTDSP2_LASTFM_EDA2.ipynb

# This script contains the API functionality for my Last.FM Exploratory Data Analysis (EDA)

### Author: Rohan Dawar
### dawar.rohan1@gmail.com
### [Website](https://www.rohandawar.com/)
### [Github](https://github.com/Rohan-Dawar/)
### [Jovian](https://jovian.ai/rohan-dawar)
### [Lastfm](https://www.last.fm/user/rmdawar)
"""

# Connecting To API
import math

# Data Handling
import numpy as np
import pandas as pd

# Web Scraping
import requests
from bs4 import BeautifulSoup

# Setting my unique API key
api_key = # Enter your Last.fm API Key Here, as a string

# Setting my lastfm username
user = # Enter your Last.fm Username here, as a string

# Checking if the request is good
r = requests.get(f'https://ws.audioscrobbler.com/2.0/?method=user.gettoptracks&user={user}&api_key={api_key}&format=json')
if not r.ok:
  print(f"Error Connecting to API: {r}")


# Function to get total pages of user's profile
def get_pages(user):
  '''
  Arguemnts:
    - 'user' (string), the lastfm user to get all tracks for
              - must be valid URL in the format: https://www.last.fm/user/{user}
  Returns:
    - 'pages' (int), number of total pages with a limit of 200 scrobbles each from the user's lastfm profile
  '''
  r = requests.get(f'https://ws.audioscrobbler.com/2.0/?method=user.getinfo&user={user}&api_key={api_key}&format=json')
  if not r.ok:
    print(f"Error Connecting to API: {r.status}")
  df = pd.read_json(r.content)
  totalscrobbles = int(df.loc['playcount'].values[0])
  return math.ceil(totalscrobbles/200)

# Function to get all tracks of user's profile, including other data in the JSON return
# This function takes an average of ~10 seconds per iteration and can take a while
def getAllTrax2(user):

  '''
  Arguemnts:
    - 'user' (string), the lastfm user to get all tracks for
              - must be valid URL in the format: https://www.last.fm/user/{user}
  '''

  alltraxdf = pd.DataFrame(columns=['artist', 'album', 'track', 'scrobbledate', 'url', 'duration', 'listeners', 'playcount', 'genre'])
  pages = get_pages(user)

  for page in trange(pages):
    p = page+1
    r = requests.get(f'https://ws.audioscrobbler.com/2.0/?method=user.getrecenttracks&user={user}&api_key={api_key}&page={p}&limit=200&format=json')
    if not r.ok:
      print(f"API Request Failed for page {p}")
    df = pd.read_json(r.content, typ='series')

    try:
      pageTracks = df['recenttracks']
    except KeyError:
      print(f'Recent Trax Error, request={r.ok}')

    for t in pageTracks['track']:
      artist = t['artist']['#text']
      track = t['name']

      # if artist + track already in alltraxdf, copy everything except for date (no need to make trackInfoRequest)

      anyPrevious = alltraxdf[(alltraxdf['artist'] == artist) & (alltraxdf['track'] == track)]

      if len(anyPrevious) > 0:
        trackInstance = anyPrevious.iloc[0]

        # scrobble date:
        try:
          date = t['date']['#text'].replace(',','')
        except KeyError:
          print(f"Date Keyerror on {t}")
          date = None

        newrow_df = pd.DataFrame({
                'artist' : artist,
                'album' : trackInstance['album'],
                'track' : track,
                'scrobbledate' : date,
                'url' : trackInstance['url'],
                'duration' : trackInstance['duration'],
                'listeners' : trackInstance['listeners'],
                'playcount' : trackInstance['playcount'],
                'genre' : trackInstance['genre']}, index=[0])
        
        alltraxdf = alltraxdf.append(newrow_df)

      else: # if no instance of the track is in the existing dataframe:
        trackInfoRequest = requests.get(f'http://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key={api_key}&artist={artist}&track={track}&format=json')

        if trackInfoRequest.ok:

          try:
            trackdf = pd.read_json(trackInfoRequest.content, typ='series')

            try:
              trackDict = trackdf['track']
            except KeyError:
              trackDict = trackdf


            # try to get toptag/genre
            try:
              toptag = trackDict['toptags']['tag'][0]['name']
            except (KeyError, IndexError):
              toptag = None

            # try to get duration
            try:
              trackDuration = trackDict['duration']
            except (KeyError, IndexError):
              trackDuration = None

            # try to get listeners (ie. amount of unique users who have recorded scrobbles of this track)
            try:
              trackListeners = trackDict['listeners']
            except (KeyError, IndexError):
              trackListeners = None

            # try to get playcount
            try:
              trackPlaycount = trackDict['playcount']
            except (KeyError, IndexError):
              trackPlaycount = None

          except ValueError: #no track info on page:
            toptag = None
            trackDuration = None
            trackListeners = None
            trackPlaycount = None

        else:
          print(f"Track Info Request={trackInfoRequest.ok}, for {artist} - {track}")

        # scrobble date:
        try:
          date = t['date']['#text'].replace(',','')
        except KeyError:
          print(f"Date Keyerror on {t}")
          date = None

        # New row containing all columns
        newrow_df = pd.DataFrame({
                  'artist' : artist,
                  'album' : t['album']['#text'],
                  'track' : track,
                  'scrobbledate' : date,
                  'url' : t['url'],
                  'duration' : trackDuration,
                  'listeners' : trackListeners,
                  'playcount' : trackPlaycount,
                  'genre' : toptag}, index=[0])
        
        # Add new row to the alltrax dataframe
        alltraxdf = alltraxdf.append(newrow_df)

  return alltraxdf

alltraxDF = getAllTrax2(user)

alltraxDF.to_csv(f'{user}.csv')


# Artist Metadata (combine) function:
def artistINFO(artistname):
  '''
  Arguemnts:
    - 'artistname' (string), derived from 'Born In' column of the artist combine
  Returns:
    - 'artistinfodf' (pandas DF), containing all available metadata from the web scraper
    - None, if metadata cannot be parsed using the web scraper
  '''
  base_url = 'https://www.last.fm/music/'
  urlArtist = artistname.replace(' ','+')
  url = base_url+urlArtist
  r = requests.get(url)
  if r.ok:
    soup = BeautifulSoup(r.text, 'html.parser')
    catalogue = soup.findAll(class_='catalogue-metadata')
    if not catalogue:
      return None
    else:
      try:
        headers = [heading.text for heading in catalogue[0].findAll('dt')]
        headers.insert(0, 'Artist')
        vals = [value.text for value in catalogue[0].findAll('dd')]
        vals.insert(0, artistname)
        metadict = dict(zip(headers, vals))
        artistinfodf = pd.DataFrame(metadict, index=[0])
        return artistinfodf
      except IndexError:
        return pd.DataFrame({'Artist' : artistname})
  else:
    print(f"Error Making Request: {r}, Artist: {artistname}")

# Creating Artist Metadata DF:
combine = pd.DataFrame(columns=['Artist'])
listOfArtists = alltraxDF['artist'].unique()

for artist in tqdm(listOfArtists):
  artdf = artistINFO(artist)
  combine = combine.append(artdf)

combine.to_csv(f'{user} - artists.csv')