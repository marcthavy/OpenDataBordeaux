import time
from datetime import datetime as dt
import schedule 
import requests as rq
import pandas as pd

#Fonction - Récupération API & Normalisation JSON/GEOJSON
def request_api(api):

    #Demande de Token : https://data.bordeaux-metropole.fr/opendata/key
    token = '' #A remplir

    #Initialisation d'un DataFrame vide en cas d'échec
    df = pd.DataFrame()

    #Chronomètre
    start = time.time()

    print('')
    print('>>>Initialisation')
    print(f'<{api.upper()}> {dt.now().strftime("%H:%M:%S")}')

    try:

        #Lancement de la Requête API
        print('<Request API> launched')
        link = f'https://data.bordeaux-metropole.fr/geojson?key={token}&typename={api}'    
        r = rq.get(link)
        
        time.sleep(2)

        #Affichage de la Réponse Serveur
        print(f'{r} {round(time.time() - start)*1000}ms')

        #Traitement en fonction de la Réponse Serveur
        if r.status_code == 200:
            
            #Normalisation JSON/GEOJSON vers DataFrame
            df = pd.json_normalize(data = r.json()['features'])
            df.rename(columns=lambda x: x.replace('properties.', ''), inplace = True)
            df['MAJ'] = dt.now()

            try:

                df.drop(['type'], axis = 1, inplace = True)
                df.drop(['geometry'], axis = 1, inplace = True)

            except:

                pass 

            print('<JSON> Normalized')

    except:

        print('<Request API> failed')

    return df

#Fonction - Enregistrement CSV (Création & Mise à jour)
def update_csv(api,path):

    #Récupération API & Normalisation JSON/GEOJSON
    df = request_api(api)

    time.sleep(2)

    #Traitement en fonction du DataFrame
    if df.empty:
    
        print('<Error> Empty Dataframe')
        pass
        
    else:

        try:
            
            #Verification du Chemin d'accès + Fichier
            with open(path): #pass
            
                #Fichier existant -> Mise à jour
                df.to_csv(path, mode = 'a', index = None, header = False, encoding = 'utf-8', sep = ';')
                print('<CSV> Updated')
                
        except IOError:
        
            #Fichier inexistant -> Création
            df.to_csv (path, mode = 'w', index = None, header = True, encoding = 'utf-8', sep = ';')
            print('<CSV> Created')

    time.sleep(2)

#Fonction - Mode Debug
def mode_debug(switch):
    
    s = 30
    n = 5
    
    if switch:
    
        schedule.every(s).seconds.do(update_csv, api, path)
        
    else:
    
        schedule.every(m).minutes.at(':00').do(update_csv, api, path)

#Chargeur
api_list = ['API'] #Liste à remplir

for api in api_list:

    #Chemin d'accès du fichier
    path = 'Desktop/Notebook/records/' + api + ' ' + dt.now().strftime('%Y-%m-%d %H:%M:%S')[5:-3].replace(':','h') + '.csv'
    
    #Schedule Mode Debug (True or False)
    mode_debug(True)

try:

    while True:
    
        schedule.run_pending()
        time.sleep(1)
        
except KeyboardInterrupt:

    print('\n>>> Parsing terminé')