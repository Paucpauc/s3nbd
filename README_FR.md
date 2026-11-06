# s3nbd

[English](README.md) | [Русский](README_RU.md) | [简体中文](README_CN.md) | [Español](README_ES.md) | Français

Serveur NBD écrit en Go qui stocke des périphériques blocs dans un stockage compatible S3. Il prend en charge plusieurs périphériques nommés, avec un seul client actif par périphérique. NBD utilise TCP sur l’interface de bouclage, sans TLS ; S3 utilise HTTP ou HTTPS, l’adressage path-style et Signature V4.

## Démarrage rapide

Vous avez besoin de Go 1.26+, d’un bucket S3 existant et de `nbd-client` sous Linux.

```sh
go build -o s3nbd .
cp config.example.json config.json
```

Modifiez `config.json` pour renseigner le point de terminaison, la région, le bucket et les tailles des périphériques, puis démarrez le serveur :

```sh
export S3_ACCESS_KEY=your-access-key
export S3_SECRET_KEY=your-secret-key
./s3nbd -config config.json
```

Dans un autre terminal, connectez les périphériques :

```sh
sudo modprobe nbd
sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N disk1
sudo nbd-client 127.0.0.1 10809 /dev/nbd1 -N disk2
```

Vous pouvez maintenant les formater et les monter avec les outils habituels de Linux. Avant d’arrêter le serveur, démontez les systèmes de fichiers et déconnectez NBD.

## Configuration

Consultez [config.example.json](config.example.json) pour un exemple complet.

| Paramètre | Fonction |
| --- | --- |
| `LogLevel` | Niveau minimal de journalisation : `debug`, `info` (par défaut), `warn` ou `error` ; insensible à la casse. Appliqué au démarrage ; une valeur invalide empêche le démarrage. Les journaux sont envoyés vers stderr avec horodatage et niveau. Les statistiques et les événements du cycle de vie utilisent info ; les connexions et commandes NBD, debug ; les avertissements, warn ; et les échecs d’opérations, error. |
| `Listen` | Adresse NBD ; doit être une IP de bouclage. |
| `Endpoint`, `Region` | Point de terminaison S3 et région utilisée pour la signature. |
| `Devices` | `Name`, `Bucket`, `Prefix` et `Size` (en octets) de chaque périphérique. |
| `BlockSize` | Octets par objet S3. Les tailles des périphériques doivent être des multiples de cette valeur. |
| `CacheBlocks` | Blocs en cache par périphérique. |
| `Workers` | Nombre d’opérations S3 simultanées par périphérique et nombre maximal de lectures NBD actives. |
| `FlushSeconds` | Intervalle de vidage du cache en arrière-plan. |
| `YandexPatch` | Active PATCH de Yandex Object Storage ; désactivé par défaut. |
| `InsecureTLS` | Désactive la vérification du certificat S3 ; désactivé par défaut. |

Définissez `S3_SESSION_TOKEN` pour utiliser des identifiants temporaires. Pour une autorité de certification personnalisée, utilisez le magasin de confiance du système ou `SSL_CERT_FILE`.

## Stockage et durabilité

**Les écritures sont acquittées en RAM. Un plantage entraîne la perte des données qui n’ont pas encore été transférées vers S3.** FLUSH et les écritures avec FUA attendent que les modifications en attente parviennent à S3. Les envois échoués restent en attente d’une nouvelle tentative et le client reçoit `EIO`.

Les vidages en arrière-plan s’exécutent toutes les `FlushSeconds` ; une déconnexion et SIGINT/SIGTERM déclenchent également un vidage. Si le vidage final échoue, le serveur se termine avec une erreur et ne peut pas conserver le contenu restant en RAM.

Chaque objet à l’emplacement `Prefix/blocks/<16 hex digits>` contient un bloc. La lecture d’un objet absent renvoie des zéros. Les écritures s’accumulent dans le cache sans requêtes GET. Le vidage d’un bloc partiellement modifié charge les données existantes avant d’envoyer le bloc complet ; une réécriture complète évite cette lecture. Les blocs ne contenant que des zéros sont supprimés au lieu d’être envoyés.

Le cache contient jusqu’à `CacheBlocks × BlockSize` octets **par périphérique**, auxquels s’ajoute la mémoire des requêtes et du SDK. Les lectures actives peuvent utiliser `Workers × 32 MiB` supplémentaires pour les tampons de réponse. Lorsque le cache est plein, il évince un bloc sans modifications en attente ou transfère les blocs modifiés vers S3. Les lectures s’exécutent en parallèle ; les écritures, les abandons de blocs et les vidages attendent la fin des lectures précédentes.

Tenez compte des contraintes suivantes :

- Ne modifiez pas les tailles des périphériques ni `BlockSize` pour des données existantes ; ces valeurs sont stockées uniquement dans la configuration.
- Utilisez un seul serveur par bucket/préfixe et ne modifiez pas ses objets depuis l’extérieur.
- Le stockage doit garantir la cohérence des lectures après écriture et la durabilité des écritures acquittées. Un vidage portant sur plusieurs objets n’est pas atomique.
- NBD utilise la négociation fixed-newstyle avec des exports nommés, READ/WRITE, FLUSH, FUA et TRIM. Les requêtes READ/WRITE sont limitées à 32 MiB ; la prise en charge de connexions multiples n’est pas annoncée.

## Libération des blocs (discard)

Exécutez `sudo fstrim /mountpoint` pour libérer les blocs inutilisés du système de fichiers. L’abandon de blocs complets entraîne des suppressions S3 par lots ; les abandons partiels mettent à zéro la plage sélectionnée. Les deux sont mis en tampon jusqu’au vidage, et les lectures renvoient immédiatement des zéros. Les suppressions échouées restent en attente d’une nouvelle tentative.

Si le versionnement S3 est activé, une suppression crée un marqueur de suppression. Utilisez une règle de cycle de vie du stockage pour supprimer les anciennes versions.

## PATCH de Yandex Object Storage

Définissez `"YandexPatch": true`, `"Region": "ru-central1"` et le point de terminaison HTTPS approprié. **Le versionnement du bucket doit être désactivé.** PATCH est une extension Yandex et ne fait pas partie du standard S3.

Ce mode envoie uniquement les plages modifiées pour les mises à jour partielles d’objets existants, ce qui évite de télécharger et d’envoyer des blocs complets. Les nouveaux objets et les réécritures complètes utilisent PUT ; les abandons complets utilisent la suppression par lots. Si PATCH renvoie `404 NoSuchKey`, le serveur effectue un PUT du bloc complet avec des zéros en dehors des plages modifiées. Les autres erreurs laissent le bloc en attente d’une nouvelle tentative et font échouer FLUSH/FUA.

Consultez la [documentation de l’API PATCH](https://yandex.cloud/ru/docs/storage/s3/api-ref/object/patch).

## Journaux et tests

Toutes les cinq secondes, le serveur journalise :

- `S3 stats` : nombre de requêtes et octets du corps envoyés/reçus par méthode HTTP, pour l’ensemble des périphériques. Inclut les nouvelles tentatives et les échecs ; exclut les en-têtes et le surcoût TCP/TLS.
- `cache stats` : objets et octets en attente de vidage par périphérique. Les octets comptabilisent les blocs entiers, y compris les suppressions en attente, et pas seulement les plages modifiées.

Exécutez les tests avec :

```sh
go test -race ./...
```
