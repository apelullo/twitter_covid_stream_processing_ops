# Twitter Covid Stream Philadelphia-Tristate
import tweepy

import pandas as pd
import math
import numpy as np
import scipy as sp

import re
import os
import sys
import json
import time

# Access credentials
CONSUMER_KEY="REDACTED"
CONSUMER_SECRET="REDACTED"
ACCESS_KEY="REDACTED"
ACCESS_SECRET="REDACTED"
# Paths
BASE_PATH = '/home/ubuntu/covid/twitter/'
DATA_PATH = BASE_PATH + 'data/'
RAW_DATA_PATH = DATA_PATH + 'raw_data/'
# Constants
DEFAULT_SLEEP = 60
# Search terms
GENERAL_TERMS = ['COVID','COVID19','covid_19','COVIDー19','COVID2019','Covid19us','2019-nCoV','COVD19','CODVID19','Covid-19',
 'nCoV2019','covid2020','Covid19pandemic','covid19out','covidkindness','covidiot','covidiots','Knowcovid','SARSCoV2',
 'corona','coronavirus','coronovirus','coronaviruspandemic','Coronaeffect','coronavirususa','coronavirusoutbreak',
 'coronavirusupdate','Coronapocalypse','coronapocolypse','CoronaOutbreak','the \’rona','the roni','virus','Pandemic',
 'MyPandemicSurvivalPlan','PreventEpidemics','publichealth','flattenthecurve','Quarantine','Quarantinelife',
 'quarantineactivities','Quarentineandchill','QuarantineAndChill','SocialDistancing','socialdistancingnow',
 'selfisolating','HarmReduction','LockDownSA','lockdowneffect','lockdownextension','lockdowndiaries','Stayhome',
 'istayhome','istayathome','Stayathome','StayTheFHome','stayhomestaysafe','StayAtHomeSaveLives','stayhomesavelives',
 'iwillsurvivechallenge','StayAtHomeChallenge','ViewFromMyWindow','TogetherAtHome','Withme','Alonetogether',
 'inthistogether','untiltomorrow','ImDoingFineBecause','Staysafe','see10send10','seeapupsendapup','Safehands',
 'handwashing','Handwashing','washyourhands','workfromhome','Peoplehavetowork','essentialworkers',
 'ThanksHealthHeroes','healthcareheroes','GetMePPE','Ppe','Mask','facemasks','Pdoh','Sdoh','hiap']
FOOD_TERMS = ['TooSmallToFail','SaveAmericanHospitality','SaveRestaurants','RestaurantRecovery',
              'ReliefForRestaurants','RallyForRestaurants','SupportLocalRestaurants','SupportLocal',
              'SupportLocalBusiness','TheGreatAmericanTakeout','CarryOut','OrderIn','CurbSide','CurbSidePickup',
              'DineLocal','StillOpen','WereOpen']
POLITICAL_TERMS = ['trumpownseverydeath','Trumpliedpeopledied','Trumpliesamericansdie','trumpliespeopledie',
                   'Wisconsinpandemicvoting','Trumpgenocide','trumppandemic','gopgenocide']
STIGMA_TERMS = ['HateIsAVirus','WashTheHate','RacismIsAVirus','IAmNotCOVID19']
CONSPIRACY_TERMS = ['filmyourhospital','filmyourhospitals','filmyourhospitalchallenge','emptyhospital','dempanic',
                    'plandemic','5gkills','5gconspiracy']
VACCINE_TERMS = ['vaccine','vaccines','vaccine','vaccines','vaccination','vaccinations','moderna','pfizer','CoronavirusVaccine','antivaxx','antivax','antivaccine','ProSafeVaccine','vaccin','CashingInOnCovid','MyBodyMyChoice','VaccineSafety','NoVaccines','Vax','NoVaccine','vaccineswork','vaccineinjury','vaccinefree','vaccineinjuryawareness','vaccinesharm','vaccinescauseadults','vaccineawareness','vaccinesdontcauseautism','vaccinehoax','DoctorsSpeakUp']
SEARCH_TERMS = GENERAL_TERMS+FOOD_TERMS+POLITICAL_TERMS+STIGMA_TERMS+CONSPIRACY_TERMS+VACCINE_TERMS
SEARCH_TERMS_LOWER = [word.lower() for word in SEARCH_TERMS]
SEARCH_STRING = '|'.join(SEARCH_TERMS).lower()

# Authentication
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
api = tweepy.API(auth)

# Class and function definitions
class CustomStreamListener(tweepy.StreamListener):
    def __init__(self):
        super().__init__()
    
    def on_status(self, status):
        # Filter tweets by keyword
        if 'extended_tweet' in status._json.keys():
            tweet_text = status._json['extended_tweet']['full_text'].lower()
        else:
            tweet_text = status.text.lower()
        keywords = [word for word in SEARCH_TERMS_LOWER if word in tweet_text]
        if len(keywords) > 0:
            process_tweet(status,keywords)

    def on_error(self, status_code):
        print('Encountered error with status code:', status_code, file=sys.stderr)
        return True # Don't kill the stream

    def on_timeout(self):
        print('Timeout...',file=sys.stderr)
        return True # Don't kill the stream

def process_tweet(status, keywords=None):
    print('Collecting status data...')
    status_json = json.dumps(status._json)
    keyword_str = ','.join(keywords)
    pd.DataFrame([[status_json,keyword_str]]).to_csv(RAW_DATA_PATH+'raw_data.csv', mode='a', header=False)
    return

# Main program with bounding box filter for philadelphia-tristate area
while(True):
    try:
        sapi = tweepy.streaming.Stream(auth, CustomStreamListener())    
        sapi.filter(locations=[-75.9814453125,39.53370327008705,-74.37744140625,40.43858586704331])
    except:
        print('Connection interrupted! Sleeping for', DEFAULT_SLEEP, 'seconds...')
        time.sleep(DEFAULT_SLEEP)
