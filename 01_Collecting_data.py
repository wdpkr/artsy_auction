import pandas as pd
import streamlit as st

st.write('# Collecting data')

st.write('This project looks into the art market. I collected data from https://www.artsy.net. '
         'It\'s a large art marketplace with data on artists and auction results. '
         'Usually, it\'s hard to get data on art auctions as it is mostly private. '
         'The aggregated data is sold by a few vendors who charge prices which make it inaccessible to the general public. '
         'With a few tricks, however, I figured out how to scrape Artsy. ')

st.write('The code on this page is not executed on the Streamlit app.')

st.write('## Importing libraries')

code = '''
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from IPython.display import HTML
import IPython
import string
from joblib import Parallel, delayed
import numpy as np
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
'''
st.code(code, language='python')

st.write('## Getting a list of artists')

st.write('First, I get a list of all artists who are on artsy.net. '
         'They conveniently put them together on https://www.artsy.net/artists/artists-starting-with-j. ')

code = '''
def get_letter_list():
    \'''
    This function returns a list of links to directories with artists grouped by their first surname letter for artsy.net. 
    Naturally, there are as many links as there are letters in the english alphabet.
    \'''
    urls = []
    for letter in list(string.ascii_lowercase):
        url = 'https://www.artsy.net/artists/artists-starting-with-{letter}'.format(letter = letter)
        urls.append(url)
    return urls

def get_artists_from_page(url):
    \'''
    This function receives a url link to a specific page in the artists catalogue from which to get a list of artists.
    An input url is unique by (letter, page number).
    This function returns an array with the name of the artist, a link to their page, and a link to a list of their auctions. 
    \'''
    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    soup_links = soup.find_all(class_="RouterLink__RouterAwareLink-sc-9hegtb-0 ArtistsByLetter__Name-sc-126slvn-1 dUegQT")
    data = []
    if len(soup_links) == 0:
        return None
    else:
        for entry in soup_links:
            name = entry.text
            link_ending = entry.get('href')
            link = 'https://www.artsy.net{link_ending}'.format(link_ending = link_ending)
            auction_link = link + '/auction-results'
            data.append([name, link, auction_link])
        return data

def get_artists_for_letter(url):
    \'''
    This function receives a url to artists catalogue from which to get a list of artists.
    An input url is unique by (letter).
    This function returns an array with the name of the artist, a link to their page, and a link to a list of their auctions. 
    \'''
    artists = []
    page = 1
    while True:
        url_with_page = url + '?page=' + str(page)
        print('Downloading artists from', url_with_page)
        artists_by_letter = get_artists_from_page(url_with_page)
        if not artists_by_letter:
            print('Letter completed')
            return artists
            break
        else:
            artists += artists_by_letter
            page += 1
            
def get_artist_data(artist_id = '4db455226c0cee664800053c'):
    \'''
    This function returns biographical data for artists given their id using an undocumented artsy.net SQL API.
    \'''
    url = 'https://metaphysics-production.artsy.net/v2'
    query = \' \nquery {\n    artist(id: "' + artist_id + '") {\n      slug\n      name\n      location\n      birthday\n      deathday\n      hometown\n      nationality\n      gender\n      href\n      blurb\n  }\n}\n \'
    r = requests.post(url, json={'query': query})
    return r.text
    
def get_list_of_artists():
    \'''
    This function scrapes a list of all artists and links to their pages from artsy.net.
    This function returns a pandas dataframe with the collected data.
    \'''
    artists = []
    urls_letter = get_letter_list()
    for url in urls_letter:
        data = get_artists_for_letter(url)
        artists += data
    df = pd.DataFrame(artists, columns = ['name', 'link', 'auction_link'])
    return df
'''
st.code(code, language='python')

st.write('Finally, we can get a full list by just running the following code: ')

code = '''
artists_list = get_list_of_artists()
artists_list.to_csv('artists_list.csv')
'''
st.code(code, language='python')

with st.echo():
    #Let's print the result
    artists_list = pd.read_csv('artists_list.csv').drop('Unnamed: 0', axis = 1)
    st.dataframe(artists_list)


st.write('## Auction dummy')

st.write('Second, not all artists have auction results. We can check it and remove artists we do not want to scrape on the next stage. '
         'It turns out that roughly 75\% of artists have never had their works auction off. Given that I use Selenium on the next step, it makes a lot of sense to cut them off and optimize the process. ')

code = '''
def get_auction_dummy(url):
    \'''
    This function receives a link to auction results page.
    This function returns a 0/1 dummy depending on whether there are any entries. 
    \'''
    auction_dummy = 0
    try:
        r = requests.get(url)
        soup = BeautifulSoup(r.text)
        if soup.find_all(class_ = 'fresnel-container fresnel-greaterThanOrEqual-sm'):
            auction_dummy = 1
    except:
        time.sleep(1)
        pass
    return auction_dummy

def get_list_of_auction_dummy(df):
    \'''
    This function receives a pandas dataframe with links scraped for each artist using get_list_of_artists().
    This function returns a pandas dataframe with auction_dummy for each artist.
    \'''
    data = []
    for index, row in df.iterrows():
        url = row.to_list()[2]
        auction_dummy = get_auction_dummy(url)
        data.append([url, auction_dummy])
    df = pd.DataFrame(data, columns = ['auction_link', 'auction_dummy'])
    return df

def df_to_batches(df, batches = 10):
    \'''
    This function breaks down a dataframe into several batches and saves them to .csv files.
    \'''
    for i in range(batches):
        batch = np.array_split(df, batches)[i]
        batch.to_csv('artists_list_batch'+str(i+1)+'.csv')

def df_to_batch_arrays(df, batches = 100):
    \'''
    This function breaks down a dataframe into several batches and returns them as a list of dataframes.
    \'''
    batch_list = []
    for i in range(batches):
        batch = np.array_split(df, batches)[i]
        batch_list.append(batch)
    return batch_list
'''
st.code(code, language='python')

st.write('I use parallel computing to save intermediate results and prevent crashing. ')

code = '''
artists_list = pd.read_csv('artists_list.csv')
artists_list = artists_list.drop('Unnamed: 0', axis = 1)

parallel_processes = 100 
#Enter the desired number of parallel processes.
#My M1 Macbook Air with 8GB RAM couldn't handle more than 100.

batch_list = df_to_batch_arrays(artists_list, batches = parallel_processes)

# This code parses the website to get batches of auction_data. Everything is done in batches to prevent crashing and memory overload.
for batch_number in range(len(batch_list)):
    batch = batch_list[batch_number]
    try:
        auction_dummy = Parallel(n_jobs=parallel_processes)(delayed(get_list_of_auction_dummy)(df_to_batch_arrays(batch, parallel_processes)[i]) for i in range(parallel_processes))
        auction_dummy = pd.concat(auction_dummy).reset_index().drop('index', axis = 1)
        auction_dummy.to_csv('auction_dummy_'+str(batch_number)+'.csv')
        print('Batch ' + str(batch_number) + ' complete')
    except Exception as e:
        print('Batch ' + str(batch_number) + ': ERROR ENCOUNTERED')
        print('_________________________')
        print(e)
        print('_________________________')

# This code creates a full auction_dummy list
auction_dummy = []
for batch_number in range(len(batch_list)):
    batch = pd.read_csv('auction_dummy_'+str(batch_number)+'.csv')
    auction_dummy.append(batch)
auction_dummy = pd.concat(auction_dummy, axis=0).reset_index().drop(['index', 'Unnamed: 0'], axis = 1)
auction_dummy.to_csv('auction_dummy.csv')

# This code writes a list of artists suitable for auction data retrieval to a new .csv file
auction_dummy = auction_dummy[auction_dummy['auction_dummy'] == 1].reset_index().drop(['index', 'auction_dummy'], axis = 1)
pd.merge(auction_dummy, artists_list, how = 'left', left_on = 'auction_link', right_on = 'auction_link').to_csv('artists.csv')
'''
st.code(code, language='python')

with st.echo():
    #Let's print the result
    auction_dummy = pd.read_csv('auction_dummy.csv').drop('Unnamed: 0', axis = 1)
    st.dataframe(auction_dummy)

with st.echo():
    artists = pd.read_csv('artists.csv').drop('Unnamed: 0', axis = 1)
    st.dataframe(artists)

st.write('## Getting auction data')

st.write('Finally, I run Selenium code to collect auction results from pages like https://www.artsy.net/artist/jean-michel-basquiat/auction-results. '
         'The two main difficulties when scraping the website are: (1) auction results are displayed as a web application, so BeautifulSoup or RoboBrowser don\'t work and (2) The website requires logging in to see auction results past first page. ')


code = '''
def login_artsy_selenium(driver, login_details = ['yijoto3801@nzaif.com', 'RXBqm4qr3n322Lq6']):
    \'''
    This function logs into artsy via selenium using login details.
    Enter login details as an array [email, password]
    \'''
    # open login page
    driver.get('https://www.artsy.net/login') 
    # select login form
    username = driver.find_elements(By.CLASS_NAME, 'Input__StyledInput-bysdh7-0.iQFbtp')[0]
    password = driver.find_elements(By.CLASS_NAME, 'Input__StyledInput-bysdh7-0.iQFbtp')[1]
    # enter login details
    username.send_keys(login_details[0]);
    password.send_keys(login_details[1]);
    # login
    login = driver.find_element(By.CLASS_NAME, 'Button__Container-sc-1bhxy1c-0.gMKEFL')
    login.click()
    # accept cookies
    try:
        time.sleep(3)
        accept_cookies = driver.find_element(By.ID, 'onetrust-accept-btn-handler')
        accept_cookies.click()
    except NoSuchElementException:
        pass
    
def get_item_auction_result(item):
    \'''
    This function gets auction data for one lot.
    This function receives a soup element.
    This function return a list with auction details.
    \'''
    
    if item.find(class_ = 'ArtistAuctionResultItem__StyledImage-ar8jz4-1 dsLTcV'):
        image = item.find(class_ = 'ArtistAuctionResultItem__StyledImage-ar8jz4-1 dsLTcV').get('src')
    else:
        image = ''
        
    name = item.find_all(class_ = 'Box-sc-15se88d-0 Text-sc-18gcpao-0 iBuAfx')[0].text
    date = item.find_all(class_ = 'Box-sc-15se88d-0 Text-sc-18gcpao-0 iBuAfx')[1].text
    price = item.find_all(class_ = 'Box-sc-15se88d-0 Text-sc-18gcpao-0 iBuAfx')[2].text.replace(',', '')
    
    if 'Artwork Info' in item.text:
        auction_house = item.find_all(class_ = 'Box-sc-15se88d-0 Text-sc-18gcpao-0 caIGcn jyjfvO')[1].text
        size = item.find_all(class_="Box-sc-15se88d-0 dZxSSH")[0].find_all(class_="Box-sc-15se88d-0 Text-sc-18gcpao-0 dORKHW")[1].text
    elif 'Artwork Dimension' in item.text:
        auction_house = item.find_all(class_ = 'Box-sc-15se88d-0 Text-sc-18gcpao-0 caIGcn jyjfvO')[0].text
        size = item.find_all(class_="Box-sc-15se88d-0 Text-sc-18gcpao-0 dORKHW")[0].text
    else:
        auction_house = ''
        size = ''
    
    if item.find(class_ = 'Box-sc-15se88d-0 Text-sc-18gcpao-0 eHKyyH jyjfvO'):
        priceUSD = item.find(class_ = 'Box-sc-15se88d-0 Text-sc-18gcpao-0 eHKyyH jyjfvO').text.replace(',', '')
    else:
        priceUSD = price
        
    try:
        price_USD = int(priceUSD)
    except ValueError:
        price_USD = None
        
    #try:
    #    work_type = BeautifulSoup(txt).find_all(class_ = 'Box-sc-15se88d-0 Flex-cw39ct-0 fvvcRt')[0].find_all(class_ = 'Box-sc-15se88d-0 Text-sc-18gcpao-0 caIGcn jyjfvO')[0].text
    #except IndexError:
    #    work_type = ''
            
    data = [name, image, date, auction_house, priceUSD[3:], size]
    return data

def collect_auction_data_from_page(driver):
    \'''
    This function collects auction data from current page.
    \'''
    lots = driver.find_elements(By.CLASS_NAME, 'Box-sc-15se88d-0.Flex-cw39ct-0.BorderBoxBase-sc-1072ama-0.BorderBox-sc-18mwadn-0.ArtistAuctionResultItem__FullWidthBorderBox-ar8jz4-0.kViSxx.bIwuel.hXQXHR.blEMrQ')
    data = []
    for lot in lots:
        txt = lot.get_attribute("innerHTML")
        data.append(get_item_auction_result(BeautifulSoup(txt)))
    return data

def get_auction_data_for_artist(url, driver):
    \'''
    This function collects auction data for an artist given their auction page url.
    This function returns an array.
    \'''
    driver.get(url)
    data = collect_auction_data_from_page(driver)
    while True:
        try:
            data += collect_auction_data_from_page(driver)
            time.sleep(0.25)
            next_button = driver.find_element(By.CLASS_NAME, 'Link-oxrwcw-0.iysjSr')
            next_button.click()
        except NoSuchElementException:
            return data
            break
        except:
            continue
            
def get_auction_data(artists):
    \'''
    This function receives a pandas dataframe with auction links for each artist.
    This function returns a pandas dataframe with auction results for each artist.
    \''' 
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    login_artsy_selenium(driver)
    auction_data = []
    
    for index, row in artists.iterrows():
        name = row['name']
        auction_link = row['auction_link']
        data = get_auction_data_for_artist(auction_link, driver)
        data = pd.DataFrame(data, columns = ['title', 'image_link', 'auction_date', 'auction_house', 'price_usd', 'size'])
        data.insert(0, 'name', name)
        data.insert(0, 'auction_link', auction_link)
        auction_data.append(data)
        
    driver.close()
            
    return pd.concat(auction_data)

artists = pd.read_csv('artists.csv').drop('Unnamed: 0', axis = 1)
number_of_batches = 1000
parallel_processes = 2
batch_list = df_to_batch_arrays(artists, number_of_batches)

for batch_number in range(44, len(batch_list)):
    batch = batch_list[batch_number]
    try:
        auction_data = Parallel(n_jobs=parallel_processes)(delayed(get_auction_data)(df_to_batch_arrays(batch, parallel_processes)[i]) for i in range(parallel_processes))
        auction_data = pd.concat(auction_data).reset_index().drop('index', axis = 1)
        auction_data.to_csv('auction_data_'+str(batch_number)+'.csv')
        print('Batch ' + str(batch_number) + ' complete')
    except Exception as e:
        continue
'''
st.code(code, language='python')

st.write('Naturally, I wasn\'t able to get all data from the website as the amount is enormous. So, I\'m going to analyse only some of the data. Of course, with enough time and computing power, all necessary data can be collected.')

code = '''
# Put the files together:
complete_batches = 44

auction_data = []
for batch_number in range(complete_batches):
    batch = pd.read_csv('auction_data_'+str(batch_number)+'.csv')
    auction_data.append(batch)
auction_data = pd.concat(auction_data, axis=0).reset_index().drop(['index', 'Unnamed: 0'], axis = 1)
auction_data.to_csv('auction_data.csv')
'''
st.code(code, language='python')
