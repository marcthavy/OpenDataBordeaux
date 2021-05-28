import time
from datetime import datetime as dt
import schedule 
import requests as rq
import pandas as pd

def translate(x):

  liane = 'blue'
  ligne = 'orange'
  citeis = 'green'
  flexo = 'grey'

  couleur = {
      'FLUIDE': 'lightgreen',
      'INCONNU': 'lightgrey',
      'EMBOUTEILLE': 'red',
      'TRAVAUX': 'road',
      'SENS_UNIQUE': 'arrow-circle-right',
      'RUE_BARREE': 'minus',
      'CIRCULATION_ALTERNEE': 'exchange',
      'TRAVAUX_TRAMWAY' : 'wrench',
      'DENSE': 'orange',
      'PARALYSE' : 'black',
      'BATEAU': 'ship',
      'BUS': 'bus',
      'TRAM': 'train',
      'BAT3': 'cadetblue',
      'Tram A': 'darkpurple',
      'Tram B': 'red',
      'Tram C': 'pink',
      'Tram D': 'purple',
      'Lianes 1': liane,
      'Lianes 2': liane,
      'Lianes 3': liane,
      'Lianes 4': liane,
      'Lianes 5': liane,
      'Lianes 7': liane,
      'Lianes 8': liane,
      'Lianes 9': liane,
      'Lianes 10': liane,
      'Lianes 11': liane,
      'Lianes 12': liane,
      'Lianes 15': liane,
      'Lianes 16': liane,
      'Ligne 20': ligne,
      'Ligne 21': ligne,
      'Ligne 22': ligne,
      'Ligne 23': ligne,
      'Ligne 24': ligne,
      'Ligne 25': ligne,
      'Ligne 26': ligne,
      'Ligne 27': ligne,
      'Ligne 28': ligne,
      'Ligne 29': ligne,
      'Ligne 30': ligne,
      'Corol 31': ligne,
      'Corol 32': ligne,
      'Corol 33': ligne,
      'Corol 34': ligne,
      'Corol 35': ligne,
      'Corol 36': ligne,
      'Corol 37': ligne,
      'Corol 38': ligne,
      'Corol 39': ligne,
      'Citéis 40': citeis,
      'Citéis 41': citeis,
      'Citéis 42': citeis,
      'Citéis 43': citeis,
      'Citéis 44': citeis,
      'Citéis 45': citeis,
      'Citéis 46': citeis,
      'Navette 47': flexo,
      'Flexo 49': citeis,
      'Flexo 50': flexo,
      'Flexo 51': flexo,
      'Flexo 52': flexo,
      'Flexo 54': flexo,
      'Flexo 55': flexo,
      'Flexo 57': flexo,
      'Citéis 63': citeis,
      'Ligne 64': citeis,
      'Ligne 67': citeis,
      'Flexo 68': citeis,
      'Ligne 71': citeis,
      'Citéis 72': citeis,
      'Ligne 73': citeis,
      'Spécifique 74': citeis,
      'Ligne 76': citeis,
      'Spécifique 77': citeis,
      'Spécifique 78': citeis,
      'Spécifique 79': citeis,
      'Ligne 80': citeis,
      'Spécifique 81': citeis,
      'Spécifique 82': citeis,
      'Ligne 83': citeis,
      'Ligne 84': citeis,
      'Spécifique 85': citeis,
      'Spécifique 86': citeis,
      'Ligne 87': citeis,
      'Spécifique 88': citeis,
      'Citéis 89': citeis,
      'Ligne 90': citeis,
      'Ligne 91': citeis,
      'Ligne 92': citeis,
      'Ligne 93': citeis,
      'Spécifique 94': citeis,
      'Spécifique 95': citeis,
      'Spécifique 96': citeis,
      'TBNight': flexo,
      'Flexo Bouliac': citeis,
      'Bus Relais A': citeis,
      'Bus Relais C': citeis,
      'Navette Arena': flexo,
      'Navette Stade Matmut Atlantique': flexo,
      'Navette Tram': citeis,
      'Navette Tram D': citeis,
  }

  for i, j in couleur.items():
    if x == i:
      return j
  return x

def swap_coords(x):

    result = []
    
    for i in x:
        if isinstance(i, list):
            result.append(swap_coords(i))
        else:
            return [x[1], x[0]]
    return result

def init_api():

    df_lignes.drop([#'gid',
                    #'libelle',
                    'ident',
                    #'vehicule',
                    'active',
                    'sae',
                    #'qualite_plus',
                    'cdate',
                    'mdate',
                    'MAJ',
                    ],axis = 1, inplace = True)

    df_lignes.rename({'libelle': 'ligne'}, axis = 1, inplace = True)

    df_arrets.drop(['geometry.type',
                    'geometry.coordinates',
                    #'gid',
                    'geom_o',
                    'geom_err',
                    'ident',
                    'groupe',
                    'numordre',
                    #'libelle',
                    'vehicule',
                    'actif',
                    'voirie',
                    'insee',
                    'source',
                    'cdate',
                    'mdate',
                    'MAJ',
                    ],axis = 1, inplace = True)

def treatment_api(dataframe):

    dataframe.drop(['geometry.type',
                       #'geometry.coordinates',
                       #'gid',
                       'geom_o',
                       'geom_err',
                       #'etat',
                       #'retard',
                       'sae',
                       'neutralise',
                       'bloque',
                       'arret',
                       'pmr',
                       'localise',
                       #'vitesse',
                       'vehicule',
                       #'statut',
                       'sens',
                       #'terminus',
                       #'rs_sv_arret_p_actu',
                       #'rs_sv_arret_p_suiv',
                       #'rs_sv_chem_l',
                       #'rs_sv_ligne_a',
                       #'rs_sv_cours_a',
                       'cdate',
                       'mdate'
                       ],axis = 1, inplace = True)

    dataframe.rename({'gid': 'id_vehicule'}, axis = 1, inplace = True)

    df = pd.merge(
        left = dataframe,
        right = df_lignes,
        how = 'left',
        left_on = 'rs_sv_ligne_a',
        right_on = 'gid'
    )

    time.sleep(1)

    df.drop(['rs_sv_ligne_a',
             'gid'
             ],axis = 1, inplace = True)

    df = pd.merge(
        left = df,
        right = df_arrets,
        how = 'left',
        left_on = 'rs_sv_arret_p_actu',
        right_on = 'gid'
    )

    time.sleep(1)

    df['rs_sv_arret_p_actu'] = df['libelle']

    df.drop(['libelle',
             'gid',
             ],axis = 1, inplace = True)

    df = pd.merge(
        left = df,
        right = df_arrets,
        how = 'left',
        left_on = 'rs_sv_arret_p_suiv',
        right_on = 'gid'
    )

    time.sleep(1)

    df['rs_sv_arret_p_suiv'] = df['libelle']

    df.drop(['libelle',
             'gid',
             ],axis = 1, inplace = True)

    df = df[df['geometry.coordinates'].notna()]
    df = df[df['statut'] != 'INCONNU'] #TERMINUS_DEP , TERMINUS_ARR
    df['geometry.coordinates'] = df['geometry.coordinates'].apply(lambda x: swap_coords(x))
    df['retard'] = df['retard'].apply(lambda x: int(x/60))
    df['icon'] = df['vehicule'].apply(lambda x: translate(x))
    df['color'] = df['ligne'].apply(lambda x: translate(x))

    return df

def request_api(api):

    df = pd.DataFrame()
    
    start = time.time()
    print('')
    print(f'<API> {api} {dt.now().strftime("%H:%M:%S")}\n<Request API> launched')

    token = ''
    link = f'https://data.bordeaux-metropole.fr/geojson?key={token}&typename={api}'
    try:
        r = rq.get(link)
        time.sleep(2)

        end = time.time()
        print(f'{r} {round(end - start)*1000}ms')

        if r.status_code == 200:
            df = pd.json_normalize(data = r.json()['features'])
            df.rename(columns=lambda x: x.replace('properties.', ''), inplace = True)
            df['MAJ'] = dt.now()
            
            try:
                df.drop(['type'], axis = 1, inplace = True)
                df.drop(['geometry'], axis = 1, inplace = True)
            except:
                pass 

            print('<JSON> Normalized')
        
        #return df
        
    except:
         pass   
        #return df

    return df

def update_csv(api,path):

    df = request_api(api)

    time.sleep(2)

    if df.empty:
        print('<Error> Empty Dataframe')
        pass
    else:

        if api == 'sv_vehic_p':
            df = treatment_api(df)

        time.sleep(2)
        
        try:
            with open(path): #pass
                df.to_csv(path, mode = 'a', index = None, header = False, encoding = 'utf-8', sep = ';')
                print('<CSV> Updated')
        except IOError:
            df.to_csv (path, mode = 'w', index = None, header = True, encoding = 'utf-8', sep = ';')
            print('<CSV> Created')

    time.sleep(2)

#Ligne commerciale
link_lignes = 'sv_ligne_a'
df_lignes = request_api(link_lignes)

#Arrêt physique sur le réseau SAEIV
link_arrets = 'sv_arret_p'
df_arrets = request_api(link_arrets)

#Initialisation API
init_api()

# 1 Temps de parcours en temps réel : ci_tpstj_a (2m30)
# 2 Station VCUB en temps réel : ci_vcub_p (2m30)
# 3 Parking hors voirie : st_park_p (2m30ss)
# 4 Déviation programmée sur une ligne SAEIV : sv_devia_l (5min)
# 5 Course d'un véhicule sur un chemin SAEIV : sv_cours_a (1min)
# 6 Capteur de trafic vélo : pc_captv_p (5min)
# 7 Véhicule en service sur le réseau SAEIV : sv_vehic_p (10sec)
# 8Etat du trafic en temps réel : ci_trafi_l (1min)
###Chemin d'une ligne SAEIV : sv_chem_l (1h)
###Événement impactant la circulation : ci_evenmt_p (15min)

API_list = ['ci_trafi_l', 'sv_vehic_p', 'pc_captv_p', 'sv_cours_a', 'sv_devia_l', 'st_park_p', 'ci_vcub_p', 'ci_tpstj_a']

for api in API_list:
    path = 'Desktop/Notebook/records/' + api + ' ' + dt.now().strftime('%Y-%m-%d %H:%M:%S')[5:-3].replace(':','h') + '.csv'
    schedule.every(5).minutes.at(':00').do(update_csv, api, path)
    #schedule.every(20).seconds.do(update_csv, api, path)

try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print('\n>>> Parsing terminé')