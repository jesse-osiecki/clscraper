from settings import *
if USE_SOCKS_PROXY: 
    print("Using proxy")
    import requesocks as requests
else:
    import requests
import pprint                                  # add this import
import sys
from bs4 import BeautifulSoup
from datetime import datetime
from multiprocessing.dummy import Pool as ThreadPool 

from elasticsearch_dsl import DocType, String, Date, Nested, Boolean, analyzer, Integer, Double
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch


#constants
attrs = {'data-pid': True} # we only want the items
location_attrs = {'data-latitude':True,'data-longitude':True, 'data-accuracy':True}
size_attrs = ("BR / ",  "Ba", "ft")
####

class Listing(DocType):
    link = String(index='not_analyzed') 
    price = Double()
    bedrooms = Double()
    bathrooms = Double()
    footage = Double()
    latitude = Double()
    longitude = Double()
    ll_accuracy = Integer()
    street_address = String(index='not_analyzed')
    description = String()
    
    class Meta:
        index = 'cl_listings' 


def fetch_one_listing(base_url, page=""):
    base = base_url + page
    reset_socks()
    session = requests.session()
    session.proxies = {'http': 'socks5://127.0.0.1:9050', 'https': 'socks5://127.0.0.1:9050'}
    resp = session.get(base, timeout=3)
    try:
        resp.raise_for_status()  # <- no-op if status==200
    except:
        pass
    return resp.content, resp.encoding

def fetch_search_results( base_url, sub, query=None, minAsk=None, maxAsk=None, bedrooms=None):
    search_params = {
        key: val for key, val in locals().items() if val is not None
    }

    base = base_url + sub
    #resp = requests.get(base, params=search_params, timeout=3)
    reset_socks()
    session = requests.session()
    session.proxies = {'http': 'socks5://127.0.0.1:9050', 'https': 'socks5://127.0.0.1:9050'}
    resp = session.get(base,timeout=10)
    resp.raise_for_status()  # <- no-op if status==200
    return resp.content, resp.encoding

def parse_source(html, encoding='utf-8'):
    parsed = BeautifulSoup(html, "html.parser", from_encoding=encoding)
    return parsed

def extract_listings(base_url, parsed):
    listings = parsed.find_all('p', class_='row', attrs=attrs)
    extracted = []

    for listing in listings:	
        size = {}
        location = {}
        price_span = None
        link = listing.find('span', class_='pl').find('a')
        html, encoding = None, None

        try:
            html, encoding = fetch_one_listing(base_url, page=link.attrs['href'])
        except:
            print("Something is wrong with listing " + base_url + link.attrs['href'])
            continue

        listing_page = parse_source(html, encoding)

        mapdiv = listing_page.findAll('div', id="map")
        try: # Try to get longitudinal data
            location = {key: mapdiv[0].attrs.get(key, '') for key in location_attrs }
        except:
            pass
        try: # Try to get address data
            mapaddrdiv = listing_page.findAll('div', class_="mapaddress")
            location['mapaddress'] = mapaddrdiv[0].contents[0].string
        except:
            pass

        price_span = listing_page.find('span', class_='price')   # add me
        try:
            attriutes = listing_page.find('p', class_='attrgroup').descendants
        except AttributeError as ae:
            print("Page  is not formatted correctly, skipping")
            print(listing_page)
            continue
        prev_child = None
        
        for child in attriutes: # collect size data carefully 
            if child in size_attrs:
                size[child.string.replace('/','').strip()] = prev_child.string
            prev_child = child
                
                
        if price_span is not None:
            price_span = price_span.string.replace('$','').strip()
        else:
            price_span = 0

        this_listing = {
            'location': location,
            'link': base_url + link.attrs['href'],
            'description': link.string.strip(),
            'price': price_span,             # and me
            'size': size  # me too
        }
        extracted.append(this_listing)
    return extracted

###main method
def run(base_url):
    # Define a default Elasticsearch client
    connections.create_connection(hosts=[elastic_host])
    es = Elasticsearch([elastic_host])  
    #connections.add_connection()
    for sub in subs:
            try:
                html, encoding = fetch_search_results(base_url, sub)
            except:
                print(base_url + " does not have a " + sub)
                continue
            doc = parse_source(html, encoding)
            listings = extract_listings(base_url, doc)
            print("Listing enumeration: " + str(len(listings)))
            #pprint.pprint(listings)
            #check to see if link is already in elastic
            for li in listings:
                l = None
                s = {'query': {'filtered': {'filter': {'term': {'link': li['link']}}}}}  
                res = es.search(index="cl_listings", body=s)
                
                if len(res['hits']['hits']) is 0:
                    print(li['link']  + " does not exist, inserting")
                    bedrooms = 0
                    bathrooms = 0
                    footage = 0
                    latitude = 0.0
                    longitude = 0.0
                    ll_accuracy = 0
                    street_address = ''

                    link = li['link']
                    try:
                        price = float(li['price'])
                    except ValueError as verr:
                        pass # do job to handle: s does not contain anything convertible to int
                    except Exception as ex:
                        pass # do job to handle: Exception occurred while converting to int
                    if 'BR' in li['size']:
                        try:
                            bedrooms = float(li['size']['BR'])
                        except ValueError as verr:
                            pass # do job to handle: s does not contain anything convertible to int
                        except Exception as ex:
                            pass # do job to handle: Exception occurred while converting to int
                    if 'Ba' in li['size']:
                        try:
                            bathrooms = float(li['size']['Ba'])
                        except ValueError as verr:
                            pass # do job to handle: s does not contain anything convertible to int
                        except Exception as ex:
                            pass # do job to handle: Exception occurred while converting to int
                    if 'ft' in li['size']:
                        try:
                            footage = float(li['size']['ft'])
                        except ValueError as verr:
                            pass # do job to handle: s does not contain anything convertible to int
                        except Exception as ex:
                            pass # do job to handle: Exception occurred while converting to int
                    if 'data-latitude' in li['location']:
                        try:
                            latitude = float(li['location']['data-latitude'])
                        except ValueError as verr:
                            pass # do job to handle: s does not contain anything convertible to int
                        except Exception as ex:
                            pass # do job to handle: Exception occurred while converting to int
                    if 'data-longitude' in li['location']:
                        try:
                            longitude = float(li['location']['data-longitude'])
                        except ValueError as verr:
                            pass # do job to handle: s does not contain anything convertible to int
                        except Exception as ex:
                            pass # do job to handle: Exception occurred while converting to int
                    if 'data-accuracy' in li['location']:
                        try:
                            ll_accuracy = int(li['location']['data-accuracy'])
                        except ValueError as verr:
                            pass # do job to handle: s does not contain anything convertible to int
                        except Exception as ex:
                            pass # do job to handle: Exception occurred while converting to int
                    if 'mapaddress' in li['location']:
                        street_address = li['location']['mapaddress']
                    description = li['description']
                    l = Listing(link=link, price=price, bedrooms=bedrooms, bathrooms=bathrooms, footage=footage, latitude=latitude, longitude=longitude, ll_accuracy=ll_accuracy, street_address=street_address, description=description) 
                    l.save()
                        


if __name__ == '__main__':
    pool = ThreadPool(13)
    results = pool.map(run, base_urls)
