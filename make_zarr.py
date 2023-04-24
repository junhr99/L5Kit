# First, you have to convert rosbag data's tf topic to csv

import pandas as pd

# pd.read_csv('file path of tf.csv')
tf_csv=pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/dmc_full_40_tf.csv')

frame_timestamp=[]
frame_timestamp_num=0

frame_temp_timestamp=[]
frame_rowindex=[]

# Select only coordinate systems that are world->ego_frame without duplication
for row_index, row in tf_csv.iterrows():
  if row[3]=="world" and row[4]=="ego_frame": 
    if row[2] not in frame_temp_timestamp:
      frame_rowindex.append(row_index)
    frame_temp_timestamp.append(row[2])
    
print(len(frame_temp_timestamp))

# Remove duplicates for timestamps
for i in frame_temp_timestamp:
  if i not in frame_timestamp:
    frame_timestamp.append(i)


frame_timestamp_num=len(frame_timestamp)
# You can choose the total number of frames to be included when you make zarr file
FRAME_TIMESTAMP_NUM=1900
frame_timestamp_num=FRAME_TIMESTAMP_NUM

#============ ego translation ============#
frame_ego_translation=[[0]*3 for i in range(frame_timestamp_num)]
num=0

for row_index, row in tf_csv.iterrows():
  if num>=FRAME_TIMESTAMP_NUM:
    break
  if row[3]=="world" and row[4]=="ego_frame" and row_index==frame_rowindex[num]:
    frame_ego_translation[num][0]=row[5]
    frame_ego_translation[num][1]=row[6]
    frame_ego_translation[num][2]=row[7]
    num=num+1

#============ ego rotation ============#
frame_ego_rotation_quad=[[0]*4 for i in range(frame_timestamp_num)]
num=0

for row_index, row in tf_csv.iterrows():
  if num>=FRAME_TIMESTAMP_NUM:
    break
  if row[3]=="world" and row[4]=="ego_frame" and row_index==frame_rowindex[num]:
    frame_ego_rotation_quad[num][0]=row[8]
    frame_ego_rotation_quad[num][1]=row[9]
    frame_ego_rotation_quad[num][2]=row[10]
    frame_ego_rotation_quad[num][3]=row[11]
    num=num+1

#============ ego rotation ============#
# xyzw expression to 3*3 matrix expression
frame_ego_rotation=[[[0]*3 for i in range (3)] for j in range(frame_timestamp_num)]

for i in range(frame_timestamp_num):
  qx=frame_ego_rotation_quad[i][0]
  qy=frame_ego_rotation_quad[i][1]
  qz=frame_ego_rotation_quad[i][2]
  qw=frame_ego_rotation_quad[i][3]

  frame_ego_rotation[i][0][0]=1 - 2*qy*qy - 2*qz*qz
  frame_ego_rotation[i][0][1]=2*qx*qy - 2*qz*qw
  frame_ego_rotation[i][0][2]=2*qx*qz + 2*qy*qw
  frame_ego_rotation[i][1][0]=2*qx*qy + 2*qz*qw
  frame_ego_rotation[i][1][1]=1 - 2*qx*qx - 2*qz*qz
  frame_ego_rotation[i][1][2]=2*qy*qz - 2*qx*qw
  frame_ego_rotation[i][2][0]=2*qx*qz - 2*qy*qw
  frame_ego_rotation[i][2][1]=2*qy*qz + 2*qx*qw
  frame_ego_rotation[i][2][2]=1 - 2*qx*qx - 2*qy*qy



#============ agent_index_interval, traffic_light_faces_index_interval ============#
# Dummy data (agent_index_interval: add index 1, traffic_light_faces_index_interval: all 0)
frame_agent_index_interval=[[1]*2 for i in range(frame_timestamp_num)]
frame_traffic_light_faces_index_interval=[[0]*2 for i in range(frame_timestamp_num)]

frame_agent_index_interval[0][0]=0

# You can choose the number of scenes and how many frames per scene
# NUMBER_OF_SCENES * NUM_FRAMES_PER_SCENE <= FRAME_TIMESTAMP_NUM(total number of frames)
NUMBER_OF_SCENES=19
NUM_FRAMES_PER_SCENE=100

scene_frame_index_interval=[[0]*2 for i in range(NUMBER_OF_SCENES)]
print(scene_frame_index_interval)
for i in range(NUMBER_OF_SCENES):
  scene_frame_index_interval[i]=[i*NUM_FRAMES_PER_SCENE,i*NUM_FRAMES_PER_SCENE+NUM_FRAMES_PER_SCENE]

scene_host=[]
for i in range(NUMBER_OF_SCENES):
  # You can choose any host
  scene_host.append("host-a013")

scene_start_time=[0]*NUMBER_OF_SCENES
scene_end_time=[0]*NUMBER_OF_SCENES

for i in range(0,NUMBER_OF_SCENES):
  scene_start_time[i]=frame_timestamp[i*NUM_FRAMES_PER_SCENE]-1000
  scene_end_time[i]=frame_timestamp[i*NUM_FRAMES_PER_SCENE+NUM_FRAMES_PER_SCENE-1]+1000

# Dummy data for agent-Test
agent_centroid=[665.03424072, -2207.51220703]
agent_extent=[4.3913283, 1.8138304, 1.5909758]
agent_yaw=1.0166751
agent_velocity=[0,0]
agent_track_id=1
agent_label_probabilities=[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

# Dummy data for traffic light-Test
tl_face_id="xbVG"
tl_traffic_light_id="/ggb"
tl_traffic_light_face_status=[0,0,1]

#===================== Until now, you made all the array data needed to make zarr =====================#



#===================== Now, save these array data and make zarr file =====================#


# In Google Colaboratory, you have to install zarr
# !pip install zarr
import zarr
import numpy as np

# These data form is from zarr_dataset.py of l5kit
PERCEPTION_LABELS = [
    "PERCEPTION_LABEL_NOT_SET",
    "PERCEPTION_LABEL_UNKNOWN",
    "PERCEPTION_LABEL_DONTCARE",
    "PERCEPTION_LABEL_CAR",
    "PERCEPTION_LABEL_VAN",
    "PERCEPTION_LABEL_TRAM",
    "PERCEPTION_LABEL_BUS",
    "PERCEPTION_LABEL_TRUCK",
    "PERCEPTION_LABEL_EMERGENCY_VEHICLE",
    "PERCEPTION_LABEL_OTHER_VEHICLE",
    "PERCEPTION_LABEL_BICYCLE",
    "PERCEPTION_LABEL_MOTORCYCLE",
    "PERCEPTION_LABEL_CYCLIST",
    "PERCEPTION_LABEL_MOTORCYCLIST",
    "PERCEPTION_LABEL_PEDESTRIAN",
    "PERCEPTION_LABEL_ANIMAL",
    "AVRESEARCH_LABEL_DONTCARE",
]
TL_FACE_LABELS = [
    "ACTIVE",
    "INACTIVE",
    "UNKNOWN",
]

FORMAT_VERSION = 2

FRAME_ARRAY_KEY = "frames"
AGENT_ARRAY_KEY = "agents"
SCENE_ARRAY_KEY = "scenes"
TL_FACE_ARRAY_KEY = "traffic_light_faces"

FRAME_CHUNK_SIZE = (10_000,)
AGENT_CHUNK_SIZE = (20_000,)
SCENE_CHUNK_SIZE = (10_000,)
TL_FACE_CHUNK_SIZE = (10_000,)

SCENE_DTYPE = [
    ("frame_index_interval", np.int64, (2,)),
    ("host", "<U16"),  # Unicode string up to 16 chars
    ("start_time", np.int64),
    ("end_time", np.int64),
]
# Information per frame (multiple per scene)
FRAME_DTYPE = [
    ("timestamp", np.int64),
    ("agent_index_interval", np.int64, (2,)),
    ("traffic_light_faces_index_interval", np.int64, (2,)),
    ("ego_translation", np.float64, (3,)),
    ("ego_rotation", np.float64, (3, 3)),
]
# Information per agent (multiple per frame)
AGENT_DTYPE = [
    ("centroid", np.float64, (2,)),
    ("extent", np.float32, (3,)),
    ("yaw", np.float32),
    ("velocity", np.float32, (2,)),
    ("track_id", np.uint64),
    ("label_probabilities", np.float32, (len(PERCEPTION_LABELS),)),
]

TL_FACE_DTYPE = [
    ("face_id", "<U16"),
    ("traffic_light_id", "<U16"),
    ("traffic_light_face_status", np.float32, (len(TL_FACE_LABELS, ))),
]

# This is how to make zarr a DirectoryStore(See zarr official document)
# If you do this, you will have 4 folders under the zarr file
# store=zarr.DirectoryStore('File path to save zarr')
store=zarr.DirectoryStore('/content/drive/MyDrive/Colab Notebooks/data/full_40_make_all_100.zarr')
root=zarr.group(store=store,overwrite=True)

# I set the agents and traffic_light_faces to have dummy data of shape 1
agents=root.zeros(AGENT_ARRAY_KEY,shape=1,chunks=AGENT_CHUNK_SIZE,dtype=AGENT_DTYPE)
frames=root.zeros(FRAME_ARRAY_KEY,shape=FRAME_TIMESTAMP_NUM,chunks=FRAME_CHUNK_SIZE,dtype=FRAME_DTYPE)
scenes=root.zeros(SCENE_ARRAY_KEY,shape=NUMBER_OF_SCENES,chunks=SCENE_CHUNK_SIZE,dtype=SCENE_DTYPE)
traffic_light_faces=root.zeros(TL_FACE_ARRAY_KEY,shape=1,chunks=TL_FACE_CHUNK_SIZE, dtype=TL_FACE_DTYPE)

a=np.array([],dtype=AGENT_DTYPE)
f=np.array([],dtype=FRAME_DTYPE)
s=np.array([],dtype=SCENE_DTYPE)
t=np.array([],dtype=TL_FACE_DTYPE)

# The process of storing data in zarr's DirectoryStore
a=np.append(a,np.array([((agent_centroid[0],agent_centroid[1]),(agent_extent[0],agent_extent[1],agent_extent[2]),agent_yaw,(agent_velocity[0],agent_velocity[1]),agent_track_id, agent_label_probabilities)],dtype=AGENT_DTYPE))
print(a)
agents[:]=a
print(agents)


for i in range(frame_timestamp_num):
  f=np.append(f,np.array([(frame_timestamp[i],(frame_agent_index_interval[i][0],frame_agent_index_interval[i][1]),(frame_traffic_light_faces_index_interval[i][0],frame_traffic_light_faces_index_interval[i][1]),(frame_ego_translation[i][0],frame_ego_translation[i][1],frame_ego_translation[i][2]),frame_ego_rotation[i])],dtype=FRAME_DTYPE))
print(f)
frames[:]=f
print(frames.shape)

for i in range(NUMBER_OF_SCENES):
  s=np.append(s,np.array([(scene_frame_index_interval[i],scene_host[i],scene_start_time[i],scene_end_time[i])],dtype=SCENE_DTYPE))
print(s)
scenes[:]=s

t=np.append(t,np.array([(tl_face_id,tl_traffic_light_id,tl_traffic_light_face_status)],dtype=TL_FACE_DTYPE))
print(t)
traffic_light_faces[:]=t

