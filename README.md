# Projet implémentation de Hadoop MapReduce "from scratch" en Python

L'énoncé du projet est disponible à l'adresse suivante : https://remisharrock.fr/courses/simple-hadoop-mapreduce-from-scratch/. \

Le principe est d'implémenter une version simple du MapReduce pour faire le calcul de compte de mots d'un fichier text réparti sur plusieurs ordinateurs connectés en réseaux. Les résultats obtenus sont ensuite comparés à loi d’Ahmdal qui est ainsi démontrée empiriquement. \

Le projet est découpé en plusieurs programmes élémentaires :
- ```MASTER.py``` qui permet de lancer le MapReduce sur plusieurs machine et de coordonner le tout.
- ```SLAVE.py``` qui effectue les opérations de Map, Shuffle et Reduce sur les workers.
- ```DEPLOY.py``` et ```CLEAN.py``` qui permettent d'automatiser le déployement des fichiers nécessaires sur les workers et de les nettoyer une fois le MapReduce terminé.
- ```machines.txt``` qui contient la liste des workers utilisée par le master.

Le dossier rendu contient des fichiers ```CVS``` qui correspondent aux temps mesurés pour effectuer les différentes étapes du MapReduce avec différentes tailles de fichier et différents nombres de workers. Le fichier ```graphs/ipynb``` contentient les calculs effectués pour comparer les résultats obtenus à la loi d'Ahmdal, et le fichier ```TP_INF727_rendu.tex``` correspond au rapport final en Latex.
