#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>

#define M 5                     // Nombre de bits pour les identifiants
#define TAG_ELECTION 1          // Message d'élection
#define TAG_RET_ELECTION 2      // Retour à l'envoyeur du message d'élection
#define TAG_LEADER 3            // Annonce du leader
#define TAG_GETID 4             // Message pour récupérer les id

// Structure qui contient les informations d'initialisation pour un pair
typedef struct {
    int id;
    int pred_p;
    int succ_p;
    int init;
} Infos;

// Structure qui contient les informations pour l'éléction de leader
typedef struct {
    int init_sender;    // Id de l'initiateur du message
    int direction;      // Indique dans quel sens le message circule dan l'anneau
    int k;              // Distance max du round
    int dist;           // Distance parcouru
} Election;


// Fonction de hachage simple
int hash_function(int value, int max_value) {
    return value % max_value;
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

int elect_leader(int id_p, int pred_rank, int succ_rank, int init) {
    int done = 0;

    int active = init;  // Indique si le noeud est actif
    int k = 1;  // Distance initiale
    int leader_id = -1;  // Identifiant du leader potentiel

    Election election;
    MPI_Status status;

    // Boucle pour élire le leader
    while (!done) {
        if (active) {
            // Comportement si le pair fait partie des candidat
            leader_id = id_p;

            election.init_sender = id_p;
            election.direction = 0;
            election.k = k;
            election.dist = 1;
            
            // Envoyer l'identifiant dans les deux directions
            MPI_Send(&election, sizeof(election), MPI_BYTE, pred_rank, TAG_ELECTION, MPI_COMM_WORLD);
            election.direction = 1;
            MPI_Send(&election, sizeof(election), MPI_BYTE, succ_rank, TAG_ELECTION, MPI_COMM_WORLD);

            for (int i=0; i<2; i++) {
                // Boucle sur la reception de message tant que le round n'est pas fini
                MPI_Recv(&election, sizeof(election), MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    
                if (status.MPI_TAG == TAG_RET_ELECTION) {
                   if (election.init_sender == id_p && election.dist < election.k) {
                        // Passe inactif si le message n'a pas fini son chemin
                        active = 0;
    
                    } else if (election.init_sender != id_p){
                        // Poursuit le chemin retour du message
                        int next_rank = election.direction ? succ_rank : pred_rank;
                        MPI_Send(&election, sizeof(election), MPI_BYTE, next_rank, TAG_RET_ELECTION, MPI_COMM_WORLD);
                    }
    
                } else if (status.MPI_TAG == TAG_ELECTION) {
                    if (election.init_sender == id_p) {
                        // Est élu leader si le message à fait le tour
                        done = 1;
                        break;
                    } else if (election.init_sender > leader_id) {
                        // Maj du leader si id plus grand
                        leader_id = election.init_sender;
                        active = 0;

                        if (election.dist == election.k) {
                            // Envoi le message retour
                            int next_rank = election.direction ? pred_rank : succ_rank;
                            election.direction = !election.direction;
                            MPI_Send(&election, sizeof(election), MPI_BYTE, next_rank, TAG_RET_ELECTION, MPI_COMM_WORLD);
                        } else {
                            // Poursuit le chemin du message
                            int next_rank = election.direction ? succ_rank : pred_rank;
                            election.dist += 1;
                            MPI_Send(&election, sizeof(election), MPI_BYTE, next_rank, TAG_ELECTION, MPI_COMM_WORLD);
                        }
                    }     
                }
            }
            k *= 2;

        } else {
            // Boucle sur la reception de messages
            MPI_Recv(&election, sizeof(election), MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    
            if (status.MPI_TAG == TAG_RET_ELECTION) {
                // Poursuit le chemin retour du message
                int next_rank = election.direction ? succ_rank : pred_rank;
                MPI_Send(&election, sizeof(election), MPI_BYTE, next_rank, TAG_RET_ELECTION, MPI_COMM_WORLD);

            } else if (status.MPI_TAG == TAG_ELECTION) {
                if (election.init_sender > leader_id) { 
                    // Maj du leader si id plus grand
                    leader_id = election.init_sender;
                }

                if (election.dist == election.k) {
                    // Envoi le message retour
                    int next_rank = election.direction ? pred_rank : succ_rank;
                    election.direction = !election.direction;
                    MPI_Send(&election, sizeof(election), MPI_BYTE, next_rank, TAG_RET_ELECTION, MPI_COMM_WORLD);
                } else {
                    // Poursuit le chemin du message
                    int next_rank = election.direction ? succ_rank : pred_rank;
                    election.dist += 1;
                    MPI_Send(&election, sizeof(election), MPI_BYTE, next_rank, TAG_ELECTION, MPI_COMM_WORLD);
                }
            } else if (status.MPI_TAG == TAG_LEADER) {
                // Récupère l'id du leader et fait suivre le message dans l'anneau
                MPI_Send(&election, sizeof(election), MPI_BYTE, succ_rank, TAG_LEADER, MPI_COMM_WORLD);
                return election.init_sender;
            }
        }
    }

    printf("\n******************** ELECTION LEADER ********************\n");
    printf("ID %d est élu leader.\n", id_p);
    election.init_sender = id_p;
    election.direction = 1;
    election.k = -1;
    election.dist = -1;
    
    // Fait circuler l'annonce du leader dans l'anneau
    MPI_Send(&election, sizeof(election), MPI_BYTE, succ_rank, TAG_LEADER, MPI_COMM_WORLD);
    MPI_Recv(&election, sizeof(election), MPI_BYTE, pred_rank, TAG_LEADER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return leader_id;
}

// Calcul des finger tables
void calculate_finger_table(int id_p, int *identifiers, int num_pairs, int *finger_table) {
    int max_id = 1 << M;  // 2^M, la taille maximale de l'espace d'identifiants

    // Parcours chaque entrée de la finger table
    for (int i = 0; i < M; i++) {
        // Calcule l'identifiant cible pour cette entrée de la finger table
        int finger_id = (id_p + (int)pow(2, i)) % max_id;

        int successor = max_id;
        int minId = max_id;

        // Trouve le successeur de finger_id
        for (int j = 0; j < num_pairs; j++) {
            // Si l'identifiant du pair est supérieur ou égal à finger_id et inférieur au successeur actuel
            // On le met à jour
            if (identifiers[j] >= finger_id && identifiers[j] < successor) {
                successor = identifiers[j];
            }

            // Maj du plus petit successeur
            if (identifiers[j] < minId) {
                minId = identifiers[j];
            }
        }
        
        // Si aucun successeur alors on a fait le tour de l'anneau donc le successeur de finger_id
        // est le plus petit successeur
        if (successor == max_id) {
            successor = minId;
        }

        // Stock le successeur dans la finger_table
        finger_table[i] = successor;
    }
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int num_pairs = size - 1;

    int id_p;
    int succ_p;
    int pred_p;
    int init;
    int *identifiers = NULL;
    int *finger_table = NULL;
    int *finger_tables = NULL;
    Infos infos;

    /********** INITIALISATION SIMULATEUR **********/
    if (rank == 0) {
        printf("******************** INITIALISATION ********************\n");

        // Initialisation des identifiants des pairs
        identifiers = (int *)malloc(num_pairs * sizeof(int));
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

        // Determine le nombre de pairs initiateurs
        int nb_initiators = (rand() % num_pairs) + 1;
        printf("Simulateur : L'ensemble des initiateurs est compose de %d pair(s) : \n", nb_initiators);

        // Sélectionner aléatoirement les pairs initiateurs
        int *initiators = (int *)malloc(num_pairs * sizeof(int));
        for (int i = 0; i < num_pairs; i++) {
            initiators[i] = 0;
        }
        for (int i = 0; i < nb_initiators; i++) {
            int init_index;
            do {
                init_index = rand() % num_pairs;
            } while (initiators[init_index] == 1);
            initiators[init_index] = 1;
            printf("\t=> Pair %d est un initiateur.\n", identifiers[init_index]);
        }
        printf("\n");

        // Envoi les infos aux pairs et les organise en anneau
        for (int i=0; i<num_pairs; i++) {
            infos.id = identifiers[i];
            infos.pred_p = identifiers[(i-1 + num_pairs) % num_pairs];
            infos.succ_p = identifiers[(i+1) % num_pairs];
            infos.init = initiators[i];

            // Envoi de la structure
            MPI_Send(&infos, sizeof(Infos), MPI_BYTE, i+1, 0, MPI_COMM_WORLD);     
        }


        free(identifiers);
        free(initiators);

    } else {
        // Reception de la structure
        MPI_Recv(&infos, sizeof(Infos), MPI_BYTE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        id_p = infos.id;
        pred_p = infos.pred_p;
        succ_p = infos.succ_p;
        init = infos.init;
        printf("ID : %d, Pred : %d, Succ : %d, Init : %d\n", id_p, pred_p, succ_p, init);    
    }
    /******** FIN INITIALISATION SIMULATEUR ********/

    if (rank != 0) {
        /********** ELECTION LEADER **********/

        int pred_rank = rank-1 == 0 ? size-1 : rank-1;
        int succ_rank = rank+1 == size ? 1 : rank+1;

        // Lance l'élection du leader
        int leader = elect_leader(id_p, pred_rank, succ_rank, init);


        /********** RECUPERATION DES ID **********/

        identifiers = (int *)malloc(num_pairs * sizeof(int));
        if (identifiers == NULL) {
            printf("Erreur malloc\n");
            return -1;
        }
        for (int i=0; i<num_pairs; i++) {
            identifiers[i] = -1;
        }

        if (leader == id_p) {
            identifiers[rank-1] = id_p;
            MPI_Send(identifiers, num_pairs, MPI_INT, succ_rank, TAG_GETID, MPI_COMM_WORLD);
            MPI_Recv(identifiers, num_pairs, MPI_INT, pred_rank, TAG_GETID, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // Affichage des identifiants
            printf("Leader: Identifiants des pairs = [ ");
            for (int i = 0; i < num_pairs; i++) {
                printf("%d ", identifiers[i]);
            }
            printf("]\n\n");

            printf("********************* FINGER TABLES *********************\n");
        } else {
            MPI_Recv(identifiers, num_pairs, MPI_INT, pred_rank, TAG_GETID, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            identifiers[rank-1] = id_p;
            MPI_Send(identifiers, num_pairs, MPI_INT, succ_rank, TAG_GETID, MPI_COMM_WORLD);
        }
       

        /********** CALCUL FINGER TABLE **********/

        // VA contenir les finger tables
        finger_tables = (int *)malloc(M * sizeof(int)* num_pairs);
        if (finger_tables == NULL) {
            printf("Erreur malloc\n");
            return -1;
        }

        if (id_p == leader) {
            for (int i=0; i<num_pairs; i++) {
                // Calcul de la finger table
                calculate_finger_table(identifiers[i], identifiers, num_pairs, finger_tables+(i*M)); 
            }
            // Fait circuler les fingers tables dans l'anneau
            MPI_Send(finger_tables, num_pairs*M, MPI_INT, pred_rank, 0, MPI_COMM_WORLD);
            MPI_Recv(finger_tables, num_pairs*M, MPI_INT, succ_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            finger_table = finger_tables+(rank-1)*M;
        } else {
            // Reception finger table
            MPI_Recv(finger_tables, num_pairs*M, MPI_INT, succ_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // Récupère uniquement sa finger table et fait suivre le message au successeur
            finger_table = finger_tables+(rank-1)*M;
            MPI_Send(finger_tables, num_pairs*M, MPI_INT, pred_rank, 0, MPI_COMM_WORLD);
        }

        // Affichage des résultats
        printf("ID: %d: Finger Table = [ ", id_p);
        for (int i = 0; i < M; i++) {
            printf("%d ", finger_table[i]);
        }
        printf("], Successeur = %d\n", finger_table[0]);

        free(identifiers);
        free(finger_tables);
    }

    MPI_Finalize();
    return 0;
}
