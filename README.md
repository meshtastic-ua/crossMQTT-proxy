# crossMQTT-proxy

MQTT proxy for cross domain communication. Comes with built-in traffic management.

- Traffic management
- Automatic ban for nodes with non-standard key
- Cross channel forwarding (MediumFast <-> LongFast)

# Configuration

```bash
cp config.json.example config.json
```

Edit `config.json` to match your configuration.


# Dependencies

```bash
pip install -r requirements.txt
```


# run

```bash
python ./main.py
```
