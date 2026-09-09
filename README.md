# AutoFastDL

Automates the fastdl process for Source engine game servers.

It watches your server's asset directories, bz2-compresses anything new or
changed, and mirrors it to an FTP fastdl host so clients download maps,
models, materials and sounds from HTTP instead of from the game server.

It is also used for two related jobs that need the same "watch a directory,
push it somewhere, expire old files" behaviour: uploading demos and syncing
Torchlight audio.

## Installation

### Docker (recommended)

Images are published to `ghcr.io/srcdslab/autofastdl`.

```yaml
services:
  autofastdl:
    image: ghcr.io/srcdslab/autofastdl:latest
    container_name: autofastdl_css_ze
    restart: unless-stopped
    volumes:
      - /home/portainer/autofastdl_css_ze/config.json:/app/config.json:ro
      - /home/portainer/css_ze/serverfiles/cstrike/maps:/home/portainer/css_ze/serverfiles/cstrike/maps
      - /home/portainer/css_ze/serverfiles/cstrike/sound:/home/portainer/css_ze/serverfiles/cstrike/sound
      - /home/portainer/css_ze/serverfiles/cstrike/materials:/home/portainer/css_ze/serverfiles/cstrike/materials
      - /home/portainer/css_ze/serverfiles/cstrike/models:/home/portainer/css_ze/serverfiles/cstrike/models
```

The config is read from `/app/config.json` by default. Mount it elsewhere and
point at it with `AUTOFASTDL_CONFIG` or `--config`.

Paths inside `sources` must match the paths **as seen inside the container**,
which is why the volumes above map to identical paths on both sides.

### From source

```bash
pip install .
autofastdl --config /etc/autofastdl/config.json
```

Requires Python 3.10 or newer.

## Configuration

Copy one of the examples and edit it:

| Example | Use case |
| --- | --- |
| [`config.example.fastdl.json`](./config.example.fastdl.json) | Maps, models, materials and sounds |
| [`config.example.demos.json`](./config.example.demos.json) | Demo upload with retention |
| [`config.example.torchlight.json`](./config.example.torchlight.json) | Torchlight audio |

The config file location is resolved in this order:

1. `--config <path>` (or `-c <path>`)
2. `AUTOFASTDL_CONFIG`
3. `./config.json`

### General

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `threads` | int | `8` | Number of workers processing I/O jobs. Each holds its own FTP connection. |
| `debug` | bool | `false` | Emit debug logs. |
| `docker` | bool | `false` | Drop timestamps from log lines, since the container runtime adds its own. |

### What gets synced

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `sources` | list | *required* | Local directories to watch, recursively. |
| `extensions` | list | *required* | File extensions to treat as assets. `"bsp"` and `".bsp"` are both accepted. |
| `ignore_names` | list | `[]` | Exact filenames to skip — typically stock game maps that every client already has. |
| `ignore_folders` | list | `[]` | Directory **names** to skip. Matches whole path components, so `workshop` does not exclude `workshop_backup`. |

### Destination

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `ftp_protocol` | string | `"ftp"` | `ftp` or `ftps`. See the note below. |
| `ftp_host` | string | *required* | FTP server hostname or address. |
| `ftp_path` | string | *required* | Remote root directory that mirrors your source tree. |
| `ftp_user` | string | *required* | Username. |
| `ftp_password` | string | *required* | Password. |

> **Use `ftps` if your server supports it.** With `ftp`, the credentials and
> every byte of every file cross the network unencrypted, on every connection
> and every reconnect. `ftps` uses explicit TLS for both the control and data
> channels.

Credentials do not have to live in the config file — see
[Environment variables](#environment-variables).

### Retention (`autoremove`)

Acts as a crontab that deletes files older than a configured age. Omit the
whole block to disable it. Local and remote are configured separately.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `autoremove::local` | object | — | Retention for files on the game server. |
| `autoremove::remote` | object | — | Retention for files on the fastdl host. |
| `autoremove::(local\|remote)::days` | int | `0` | Days to keep files. |
| `autoremove::(local\|remote)::minutes` | int | `0` | Minutes to keep files. |
| `autoremove::(local\|remote)::seconds` | int | `0` | Seconds to keep files. |
| `autoremove::(local\|remote)::remove` | bool | `false` | Actually delete. With `false`, expiry is logged only. |
| `autoremove::(local\|remote)::autoclean` | bool | `false` | Re-check this side during reconciliation passes. |
| `autoremove::remote::startup_clean` | bool | `false` | Sweep the remote once at startup. |
| `autoremove::priority` | string | — | Which side's timestamp decides age: `local` or `remote`. |
| `autoremove::after_upload` | bool | `false` | Delete the local file once it has been uploaded. |

`days`, `minutes` and `seconds` **add together**. If they sum to zero, nothing
is ever considered outdated — that is the "no retention configured" case, not
"expire everything".

### Tuning

Defaults are fine for most setups.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `reconcile_debounce_seconds` | int | `30` | Quiet period before a full reconciliation pass runs. Raise it if you routinely drop large batches of files at once. |
| `created_grace_seconds` | int | `5` | How long to wait for a write to finish before uploading a file that arrived without a close event (for example moved in from outside a watched directory). |
| `queue_high_water` | int | `10000` | Queue depth above which new events are deferred to the next reconciliation pass instead of being queued. |

### Environment variables

Environment variables override the config file, so credentials can come from
Docker/Kubernetes secrets or CI variables instead of a file on disk.

| Variable | Overrides |
| :--- | :--- |
| `AUTOFASTDL_CONFIG` | Config file path |
| `AUTOFASTDL_FTP_PROTOCOL` | `ftp_protocol` |
| `AUTOFASTDL_FTP_HOST` | `ftp_host` |
| `AUTOFASTDL_FTP_PATH` | `ftp_path` |
| `AUTOFASTDL_FTP_USER` | `ftp_user` |
| `AUTOFASTDL_FTP_PASSWORD` | `ftp_password` |
| `AUTOFASTDL_DEBUG` | `debug` |
| `AUTOFASTDL_DOCKER` | `docker` |

So the config file can be committed without credentials in it:

```yaml
    environment:
      AUTOFASTDL_FTP_USER: ${FASTDL_USER}
      AUTOFASTDL_FTP_PASSWORD: ${FASTDL_PASSWORD}
```

## How it works

On startup every source directory is walked and compared against the remote,
then the process watches for changes.

- A new or modified file is compressed to `.bz2` and uploaded.
- Uploads go to a temporary name and are renamed into place, so an interrupted
  transfer never leaves a truncated archive for clients to download, and a
  failed upload leaves the previous file serving.
- Renames and deletions are mirrored to the remote.
- Reconciliation passes are debounced: a burst of filesystem activity produces
  one pass once things go quiet, rather than one per event.

The remote layout mirrors the source tree relative to each source directory's
parent, so `<source>/maps/de_foo.bsp` becomes `<ftp_path>/maps/de_foo.bsp.bz2`.

## Development

```bash
python -m venv venv && . venv/bin/activate
pip install -e ".[dev]"

pytest              # tests
flake8 src tests    # lint
mypy                # type-check
black src tests     # format
isort src tests     # import order
```

CI runs all of the above on Python 3.10, 3.11 and 3.12; the image is only
published if they pass.

## Licence

LGPL-3.0-or-later. See [LICENSE](./LICENSE) (and [COPYING](./COPYING), which
the LGPL incorporates by reference).
