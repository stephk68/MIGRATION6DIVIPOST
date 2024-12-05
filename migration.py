import pymysql
from pymongo import MongoClient

import uuid

# def generate_unique_codes(data):
#     return [str(uuid.uuid4())[:8] for _ in data]  # Génère des codes uniques courts (8 caractères)

def mysql_connexion():
    mysql_conn = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='divipost'
    )
    return mysql_conn.cursor(pymysql.cursors.DictCursor)

def mongo_connexion(collection_name):
    mongo_client = MongoClient('mongodb+srv://aymeric:aymeric@cluster0.f5dc9aa.mongodb.net/PHENICIA?retryWrites=true&w=majority')
    mongo_db = mongo_client['DIVIPOST']
    # Créer la collection POSTE si elle n'existe pas
    if collection_name not in mongo_db.list_collection_names():
        mongo_db.create_collection(collection_name)
    # Vide la collection 
    mongo_db[collection_name].delete_many({})
    
    return mongo_db[collection_name]


def close_mongo_connexion():
     mongo_client = MongoClient('mongodb+srv://aymeric:aymeric@cluster0.f5dc9aa.mongodb.net/PHENICIA?retryWrites=true&w=majority')
     mongo_client.close()

def migrate_data():
    # Connexion à MySQL
   
    mysql_cursor = mysql_connexion()
    
    # Connexion à MongoDB
    
    collection = mongo_connexion('POSTE')
    
    collection_ouvrage = mongo_connexion('OUVRAGE')
    
    # Création d'un index unique sur "previous_id"
    collection.create_index('previous_id', unique=True)
    
    # Pagination MySQL
    offset = 0
    BATCH_SIZE = 100  # Taille du lot
    while True:
        # Récupérer un lot de données depuis la table POSTE
        mysql_cursor.execute('SELECT * FROM poste LIMIT %s OFFSET %s', (BATCH_SIZE, offset))
        rows = mysql_cursor.fetchall()
        if not rows:
            break
        
        documents = []
        ouvrages_documents = []
        for row in rows:
            poste_id = row['id']
            
            # Récupérer les ouvrages liés
            mysql_cursor.execute('SELECT * FROM ouvrage WHERE poste_id = %s', (poste_id,))
            ouvrages = mysql_cursor.fetchall()
            
            # Récupérer les annexes d'ouvrages liées
            mysql_cursor.execute('SELECT * FROM annexe_ouvrage WHERE poste_id = %s', (poste_id,))
            annexes_ouvrages = mysql_cursor.fetchall()
            
            # Transformer les ouvrages
            ouvrages = [
                {
                    'previous_id': ouvrage['id'],
                    'libelle': ouvrage['libelle'],
                    'is_active': bool(ouvrage['isactif']),
                    'GMAO': ouvrage['GMAO'],
                    'indice': ouvrage['indice'],
                    'annexes': False,
                    'ouvrage_id': None,
                    'poste_id': poste_id
                }
                for ouvrage in ouvrages
            ]
            
            # Transformer les annexes d'ouvrages
            annexes_ouvrages = [
                {
                    'previous_id': annexe_ouvrage['id'],
                    'libelle': annexe_ouvrage['libelle'],
                    
                    'is_active': bool(annexe_ouvrage['isactif']),
                    'GMAO': annexe_ouvrage['GMAO'],
                    'indice': annexe_ouvrage['indice'],
                    'annexes': True,
                    'ouvrage_id': annexe_ouvrage['ouvrage_id'],
                    'poste_id': poste_id
                }
                for annexe_ouvrage in annexes_ouvrages
            ]
            
            # Fusionner ouvrages et annexes
            ouvrages.extend(annexes_ouvrages)
            
            # Créer le document final pour POSTE
            documents.append({
                'previous_id': poste_id,
                'libelle': row['libelle'],
                'localisation': row['localisation'],
                'tension': row['tension'],
                'is_active': bool(row['isactif']),
                'GMAO': row['GMAO'],
                'dr_id': row['dr_id'],
                'ouvrages': ouvrages
            })
            for ouvrage in ouvrages:
                mysql_cursor.execute('SELECT * FROM equipement WHERE ouvrage_id = %s', (ouvrage['previous_id'],))
                equipements = mysql_cursor.fetchall()
                if ouvrage['annexes'] == False:
                    ouvrages_documents.append({
                        'previous_id': ouvrage['previous_id'],
                        'libelle': ouvrage['libelle'],
                        'is_active': ouvrage['is_active'],
                        'GMAO': ouvrage['GMAO'],
                        'indice': ouvrage['indice'],
                        'annexes': ouvrage['annexes'],
                        'ouvrage_id': ouvrage['ouvrage_id'],
                        'poste_id': ouvrage['poste_id'],
                        'equipements': [
                            {
                                'previous_id': equipement['id'],
                                'libelle': equipement['libelle'],
                                
                                'is_active': bool(equipement['isactif']),
                                'GMAO': equipement['GMAO'],
                                'indice': equipement['indice'],
                            }
                            for equipement in equipements
                        ],
                        'poste': {
                            'previous_id': row['id'],
                            'libelle': row['libelle'],
                            
                            'localisation': row['localisation'],
                            'tension': row['tension'],
                            'is_active': bool(row['isactif']),
                            'GMAO': row['GMAO'],
                            'dr_id': row['dr_id'],
                        }
                    })
                else:
                    mysql_cursor.execute('SELECT * FROM ouvrage WHERE id = %s', (ouvrage['ouvrage_id'],))
                    exist_ouvrage = mysql_cursor.fetchone()
                    ouvrages_documents.append({
                        'previous_id': ouvrage['previous_id'],
                        'libelle': ouvrage['libelle'],
                        'is_active': ouvrage['is_active'],
                        'GMAO': ouvrage['GMAO'],
                        'indice': ouvrage['indice'],
                        'annexes': ouvrage['annexes'],
                        'ouvrage_id': {
                            'previous_id': exist_ouvrage['id'],
                            'libelle': exist_ouvrage['libelle'],
                            'GMAO': exist_ouvrage['GMAO'],
                            'indice': exist_ouvrage['indice'],
                            'is_active': bool(exist_ouvrage['isactif']),
                                       } 
                        if exist_ouvrage else None,
                        'poste_id': ouvrage['poste_id'],
                        'equipements': [
                            {
                                'previous_id': equipement['id'],
                                'libelle': equipement['libelle'],
                                
                                'is_active': bool(equipement['isactif']),
                                'GMAO': equipement['GMAO'],
                                'indice': equipement['indice'],
                            }
                            for equipement in equipements
                        ],
                        'poste': {
                            'previous_id': row['id'],
                            'libelle': row['libelle'],
                            'localisation': row['localisation'],
                            'tension': row['tension'],
                            'is_active': bool(row['isactif']),
                            'GMAO': row['GMAO'],
                            'dr_id': row['dr_id'],
                        }
                    })
            print(f"Migration du poste terminée: {row['libelle']}")
        
        # Insérer les documents dans MongoDB
        if documents:
            collection.insert_many(documents)
        if ouvrages_documents:
            collection_ouvrage.insert_many(ouvrages_documents)
        # Passer au prochain lot
        offset += BATCH_SIZE
    
    # Fermer les connexions
    mysql_cursor.close()
    mysql_connexion().close()
    close_mongo_connexion()



if __name__ == "__main__":
    migrate_data()
