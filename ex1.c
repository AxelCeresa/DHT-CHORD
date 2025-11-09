#include <mpi.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

// Paramètres
#define M 5  // Nombre de bits pour les identifiants
#define TAG_LOOKUP 1
#define TAG_QUIT 2

// Structure qui contient les informations d'initialisation pour un pair
typedef struct {
    int id;
    int finger_table[M];
    int finger_table_rank[M];
} Infos;

// Fonction de hachage simple
int hash_function(int value, int max_value) {
    return value % max_value;
}

// Calcul des finger tables
void calculate_finger_table(int id_p, int *identifiers, int num_pairs, int *finger_table, int *finger_table_rank) {
    int max_id = 1 << M;  // 2^M, la taille maximale de l'espace d'identifiants

    // Parcours chaque entrée de la finger table
    for (int i = 0; i < M; i++) {
        // Calcule l'identifiant cible pour cette entrée de la finger table
        int finger_id = (id_p + (int)pow(2, i)) % max_id;

        int successor = max_id+1;
        int successor_rank;
        int minId = max_id+1;
        int min_rank;

        // Trouve le successeur de finger_id
        for (int j = 0; j < num_pairs; j++) {
            // Si l'identifiant du pair est supérieur ou égal à finger_id et inférieur au successeur actuel
            // On le met à jour
            if (identifiers[j] >= finger_id && identifiers[j] < successor) {
                successor = identifiers[j];
                successor_rank = j+1;
            }

            // Maj du plus petit successeur
            if (identifiers[j] < minId) {
                minId = identifiers[j];
                min_rank = j+1;
            }
        }
        
        // Si aucun successeur alors on a fait le tour de l'anneau donc le successeur de finger_id
        // est le plus petit successeur
        if (successor == max_id+1) {
            successor = minId;
            successor_rank = min_rank;
        }

        // Stock le successeur dans la finger_table
        finger_table[i] = successor;
        // Stock le rank du successeur trouvé
        finger_table_rank[i] = successor_rank;
    }
}

// Trouve le plus grand finger possible pour la cle k
int find_next(int id_p, const int *finger_table, int key) {
    int rank = -1;
    int max = -1;
    for (int i=1; i<M; i++) {
        // vérification des bornes ]a, b]
        if (finger_table[i] < id_p) {
            // Interval normal (a < b)
            if (finger_table[i] < key && key <= id_p && finger_table[i] > max) {
                max = finger_table[i];
                rank = i;
            }
        } else {
            // Interval qui traverse l'anneau (a > b)
            if ((finger_table[i] < key || key <= id_p) && finger_table[i] > max) {
                max = finger_table[i];
                rank = i;
            }
        }
    }
    return rank;
}

// Cherche la cle key dans la DHT
void lookup(int id_p, const int *finger_table, const int *finger_table_rank, int key) {
    if (key == id_p) {
        // Le pair est la cle
        printf("\t=> Le pair %d possède la clé.\n",id_p);
        MPI_Send(&id_p, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    } else {
        int next = find_next(id_p, finger_table, key);
        if (next == -1) {
            // Le successeur possede la cle
            int succ_p = finger_table[0];
            printf("\t=> Le successeur de %d possède la clé.\n", id_p);
            MPI_Send(&succ_p, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        } else {
            // Transfer du lookup au finger trouvé
            printf("\t=> Transfert du lookup au pair %d.\n", finger_table[next]);
            MPI_Send(&key, 1, MPI_INT, finger_table_rank[next], TAG_LOOKUP, MPI_COMM_WORLD);
        }   
    }
}

// Vérifie qu'un id n'existe pas déjà dans la liste des identifiants 
int identifier_exists(int id, int *identifiers, int size) {
    for (int i = 0; i < size; i++) {
        if (identifiers[i] == id) {
            return 1;
        }
    }
    return 0;
}

int main(int argc, char **argv) {
    MPI_Status status;
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int num_pairs = size - 1; 

    int id_p;
    int succ_p;
    int *identifiers = NULL;
    int *finger_table = NULL;
    int *finger_table_rank = NULL;
    Infos infos;

    /********** INITIALISATION **********/
    if (rank == 0) {
        printf("******************** INITIALISATION ********************\n");

        identifiers = (int *)malloc(num_pairs * sizeof(int));

        // Initialisation des identifiants des pairs
        srand(time(NULL));
        for (int i = 0; i < num_pairs; i++) {
            int id;
            do {
                id = hash_function(rand(), 1 << M);
            } while (identifier_exists(id, identifiers, i));
            identifiers[i] = id;
        }

        // Affichage des identifiants
        printf("Simulateur: Identifiants des pairs = [ ");
        for (int i = 0; i < num_pairs; i++) {
            printf("%d ", identifiers[i]);
        }
        printf("]\n");

        // Calcul et envoi des finger tables et des ids
        for(int i = 0; i < num_pairs; i++) {
            infos.id = identifiers[i];

            // Calcul de la finger table
            calculate_finger_table(identifiers[i], identifiers, num_pairs, infos.finger_table, infos.finger_table_rank);

            // Affichage des résultats pour chaque pair
            printf("Pair %d (ID: %d): Finger Table = [ ", i+1, identifiers[i]);
            for (int i = 0; i < M; i++) {
                printf("%d ", infos.finger_table[i]);
            }
            printf("], Successeur = %d\n", infos.finger_table[0]);

            // Envoi de la structure contenant l'id, la finger table et les rangs
            MPI_Send(&infos, sizeof(Infos), MPI_BYTE, i+1, 0, MPI_COMM_WORLD);
        }

        free(identifiers);

    } else {
        finger_table =  (int *)malloc(M * sizeof(int));
        finger_table_rank = (int *)malloc(M * sizeof(int));

        // Reception de la structure
        MPI_Recv(&infos, sizeof(Infos), MPI_BYTE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        id_p = infos.id;
        succ_p = infos.finger_table[0];

        // Copie les infos reçues dans les tableaux locaux
        for (int i = 0; i < M; i++) {
            finger_table[i] = infos.finger_table[i];
            finger_table_rank[i] = infos.finger_table_rank[i];
        }
    }
    /******** FIN INITIALISATION ********/

    /************** LOOKUP **************/
    if (rank == 0) {
        printf("\n\n************************ LOOKUP ************************\n");
        // Choisir aléatoirement un identifiant de pair et une clé
        int random_pair_index = rand() % num_pairs;
        int random_pair_id = identifiers[random_pair_index];
        int key_to_find = rand() % (1 << M);
        printf("Simulateur: Recherche de la clé %d par le pair d'ID %d.\n\n", key_to_find, random_pair_id);

        // Envoyer la requête de recherche au pair sélectionné
        MPI_Send(&key_to_find, 1, MPI_INT, random_pair_index + 1, TAG_LOOKUP, MPI_COMM_WORLD);

        // Recevoir le résultat de la recherche
        int responsible_p;
        MPI_Recv(&responsible_p, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("\nLe pair responsable de la clé %d est le pair avec l'ID %d.\n\n", key_to_find, responsible_p);

        // Envoyer un message de terminaison à tous les processus
        for (int i = 1; i < size; i++) {
            int quit = 1;
            MPI_Send(&quit, 1, MPI_INT, i, TAG_QUIT, MPI_COMM_WORLD);
        }

    } else {
        int responsible_p;
        int recv;
        while (1) {
            // Boucler sur la reception d'un message
            MPI_Recv(&recv, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

            if (status.MPI_TAG == TAG_QUIT) { 
                // Quitter si message de terminaison
                break; 

            } else if (status.MPI_TAG == TAG_LOOKUP) { 
                // Lancer lookup si message de recherche
                lookup(id_p, finger_table, finger_table_rank, recv);                
            }

        }
    }
    /************ FIN LOOKUP ************/
    
    if (rank != 0) {
        free(finger_table);
        free(finger_table_rank);
    }

    MPI_Finalize();
    return 0;
}