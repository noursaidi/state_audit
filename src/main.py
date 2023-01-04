
import json
import copy
import hashlib
import base64
import os

from google.cloud import bigquery

class State:
  copy = {}

  REPLACE_TIMESTAMP = 'replaced'

  last_config = ''
  has_units = 0
  has_points = 0
  make_model = ''
  has_status = 0

  def __init__(self, payload):
    self.payload = payload

    self.json = json.loads(payload)
    self.timestamp = self.json.get('timestamp')

    # Last Config
    self.last_config = self.json.get('system', {}).get('last_config', '')

    # Normalise and hash
    self.json_normalised = copy.deepcopy(self.json)
    self.normalise_key(self.json_normalised, 'timestamp', 'removed')
    self.normalised = json.dumps(self.json_normalised, sort_keys=True)
    self.hash = hashlib.sha256(self.normalised.encode('utf-8')).hexdigest()

    # UNITS
    self.units = self.get_units()
    self.has_units = True if len(self.units) > 0 else False

    # Status
    self.has_status = self.check_status()

    # Hardware
    self.hardware = self.get_hardware()
    self.make = self.hardware.get('make')
    self.model = self.hardware.get('model')
    self.sku = self.hardware.get('sku')
    self.rev = self.hardware.get('rev')
    self.has_make_model = True if self.model else False

    #self.software = self.get_software
    self.software = self.get_software()
    self.has_software = True if self.software else False

    # points
    self.has_points = self.check_has_points()

  def check_has_points(self):
    points = self.json.get('pointset', {}).get('points', {})
    if len(points) > 0:
      return True
    return False

  # Software
  def get_hardware(self):
    hardware = {}
    system = self.json.get('system', {})
    if system.get('make_model'):
      hardware['make'] = 'unknown'
      hardware['model'] = system.get('make_model')
    else:
      hardware = system.get('hardware', {})
    
    return hardware

  def get_software(self):
    legacy_version = str(self.json.get('system', {}).get('firmware', {}).get('version'))
    if legacy_version:
      return legacy_version
    return json.dumps(self.json.get('system', {}).get('software'),sort_keys=True)

  def check_status(self):
    for a in self.iterate_self(self.json):
      path = list(a)
      if 'status' in path or 'statuses' in path :
        return True
    return False
  
  def iterate_self(self, start):
     for k, v in start.items():
      yield (k, v)
      if isinstance(v, dict):
        for p in self.iterate_self(v):
            yield (k, *p)

  def normalise_key(self, target, key, value):
    for k, v in target.items():
      if key == k:
        target[k] = value
      elif isinstance(v, dict):
        self.normalise_key(v, key, value)

  def get_units(self):
    points = self.json.get('pointset', {}).get('points', {})
    units = []
    for point in points:
      if 'units' in point:
        units.append(point['units'])
    return units

def hello_pubsub(event, context):
  client = bigquery.Client()
  PROJECT_ID = os.environ.get('PROJECT_ID','')
  DATASET_ID = os.environ.get('DATASET_ID','')
  TABLE = os.environ.get('TABLE','')

  TABLE_ID = f'{PROJECT_ID}.{DATASET_ID}.{TABLE}'
  error = False
  payload = base64.b64decode(event['data']).decode('utf-8')
  try:
    state = State(payload)
    sha256 = state.hash
    last_config = state.last_config
    has_units = state.has_units
    has_status = state.has_status
    has_make_model = state.has_make_model
    make = state.make
    model = state.model
    sku = state.sku
    rev = state.rev
    software = state.software
    has_software = state.has_software
    units = ",".join(state.units)
    has_points = state.has_points
  except:
    error = True

  device_id = event['attributes'].get('deviceId')
  registry_id = event['attributes'].get('deviceRegistryId')
  gateway_id = event['attributes'].get('gatewayId')
  device_numeric_id = event['attributes'].get('deviceNumId')

  ip_device = True if gateway_id else False

  timestamp = context.timestamp

  if error:
     row_to_insert = {
        "timestamp": timestamp,
        "device_id": device_id, 
        "device_registry_id": registry_id,  
        "device_numeric_id": device_numeric_id,
        "gateway_id": gateway_id,
        "ip_device": 1 if ip_device else 0, 
        "error": 1
    }
  else:
    row_to_insert = {
        "timestamp": timestamp,#
        "device_id": device_id, #
        "device_registry_id": registry_id,  #
        "device_numeric_id": device_numeric_id,#
        "gateway_id": gateway_id,#
        "ip_device": 1 if ip_device else 0,  #
        "message_hash": sha256,#
        "last_config": last_config,
        "has_make_model": 1 if has_make_model else 0, #
        "has_software": 1 if has_software  else 0,#
        "has_points": 1 if has_points else 0,#
        "has_units": 1 if has_units else 0, #
        "has_status": 1 if has_status else 0,
        "make": make,#
        "model": model,#
        "sku": sku,#
        "rev": rev,#
        "software": software,#
        "units": units,
        "error": 0
    }

  errors = client.insert_rows_json(TABLE_ID, [row_to_insert])  # Make an API request.
  if errors != []:
    raise Exception("Encountered errors while inserting rows: {}".format(errors))
  