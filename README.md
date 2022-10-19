# AutoFastDL

Automates the fastdl process for steam games

## Configuration

### FastDL
You can find the main configuration under a file named [config.json](./config.example.fastdl.json)

### Demos
You can find the main configuration under a file named [config.json](./config.example.demos.json)

### Torchlight
You can find the main configuration under a file named [config.json](./config.example.torchlight.json)

### Parameters

The following parameters are those you can find in the [config.json](./config.example.demos.json), the inner parameters are separated with `::`

| Parameter | Function |
| :----: | --- |
| `threads` | Number of threads processing i/o jobs |
| `debug` | If set to true, display the debug logs |
| `docker` | If set to true, adapts the logger output format for docker |
| `autoremove` | Acts as a crontab that deletes specific files |
| `autoremove::local` | Local configuration for the autoremove function |
| `autoremove::remote` | Remote configuration for the autoremove function |
| `autoremove::(local/remote)::days` | How much days do we keep files |
| `autoremove::(local/remote)::minutes` | How much minutes do we keep files |
| `autoremove::(local/remote)::seconds` | How much seconds do we keep files |
| `autoremove::(local/remote)::remove` | If set to true, deletes the file in the autoremove functions |
| `autoremove::(local/remote)::autoclean` | If set to true, reparses all directories whenever a local file change occurs |
| `autoremove::remote::startup_clean` | If set to true, autocleans the remote files |
| `autoremove::priority` | Gives the datetime comparison priority to either `local` or `remote` |
| `autoremove::after_upload` | If set to true, removes the local file after it has been uploaded |

## Docker-compose example

```
version: '3.7'

services:
  autofastdl:
    image: ghcr.io/srcdslab/autofastdl:latest
    container_name: autofastdl_css_ze
    volumes:
      - /home/portainer/autofastdl_css_ze/config.json:/app/config.json
      - /home/portainer/css_ze/serverfiles/cstrike/maps:/home/portainer/css_ze/serverfiles/cstrike/maps
      - /home/portainer/css_ze/serverfiles/cstrike/sound:/home/portainer/css_ze/serverfiles/cstrike/sound
      - /home/portainer/css_ze/serverfiles/cstrike/materials:/home/portainer/css_ze/serverfiles/cstrike/materials
      - /home/portainer/css_ze/serverfiles/cstrike/models:/home/portainer/css_ze/serverfiles/cstrike/models
```
