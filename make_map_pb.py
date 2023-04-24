# First, you have to convert rosbag data's road topic to csv

import pandas as pd
# pd.read_csv('file path of road.csv')
road_csv=pd.read_csv('/content/drive/MyDrive/Colab Notebooks/data/road.csv')


from numpy.lib.function_base import delete
lane_num=[]
for i in range(road_csv.shape[1]):
  road_csv_col_split=road_csv.columns[i].split('.')
  if len(road_csv_col_split)>=4:
    lane_num.append(road_csv_col_split[1])


delete_double_lane_num=[]
for i in lane_num:
  if i not in delete_double_lane_num:
    delete_double_lane_num.append(i)
print(delete_double_lane_num)

print(len(delete_double_lane_num))

delete_double_lane_num.remove('route')
delete_double_lane_num.remove('goal')

# The number of lanenet (lanes0~lanes291: lane_number=292)
lane_number=len(delete_double_lane_num)

# open('file path of road.csv','r')
f_read=open('/content/drive/MyDrive/Colab Notebooks/data/road.csv','r')
lines=f_read.readlines()
f_read.close()

want_road=[]
want_road_val=[]

for i in range(road_csv.shape[1]):
  road_csv_col_split=road_csv.columns[i].split('.')
  if len(road_csv_col_split)>=4 and road_csv_col_split[1][0:5]=='lanes':
    if road_csv_col_split[2][0:4]=='left' or road_csv_col_split[2][0:5]=='right':
      want_road.append(road_csv.columns[i])
      want_road_val.append(lines[1].split(",")[i])


want_road_split=[]
for i in range(len(want_road)):
  want_road_split.append(want_road[i].split('.'))

print(want_road_split)

# Download 'road_network_pb2' from l5kit and place it in the same location as this file
import road_network_pb2
mapp=road_network_pb2.MapFragment()
# You can choose any mapp.name
mapp.name="~/Downloads/test9_company_data_cumsum.pb"
temp=0

for i in range(lane_number):
  elements=mapp.elements.add()
  to_byte=str(i)
  elements.id.id=bytes(to_byte.encode())
  # Reference point of map(lat*10^7, lng*10^7 : int)
  elements.element.lane.geo_frame.origin.lat_e7=-4401
  elements.element.lane.geo_frame.origin.lng_e7=-895

  temp_lx=0
  temp_ly=0
  temp_rx=0
  temp_ry=0

  while(True):
    if temp>=len(want_road):
      break
    if int(want_road_split[temp][1][5:])!=i:
      break
    
    if want_road_split[temp][2][0:4]=='left':

      elements.element.lane.left_boundary.vertex_deltas_x_cm.append(int(float(want_road_val[temp])*100)-temp_lx)
      
      temp_lx=int(float(want_road_val[temp])*100)
      temp=temp+1

      elements.element.lane.left_boundary.vertex_deltas_y_cm.append(int(float(want_road_val[temp])*100)-temp_ly)
      
      temp_ly=int(float(want_road_val[temp])*100)
      temp=temp+1

      elements.element.lane.left_boundary.vertex_deltas_z_cm.append(0)
      temp=temp+1

    if want_road_split[temp][2][0:5]=='right':

      elements.element.lane.right_boundary.vertex_deltas_x_cm.append(int(float(want_road_val[temp])*100)-temp_rx)
      
      temp_rx=int(float(want_road_val[temp])*100)
      temp=temp+1

      elements.element.lane.right_boundary.vertex_deltas_y_cm.append(int(float(want_road_val[temp])*100)-temp_ry)
      
      temp_ry=int(float(want_road_val[temp])*100)
      temp=temp+1

      elements.element.lane.right_boundary.vertex_deltas_z_cm.append(0)
      temp=temp+1


serialized=mapp.SerializeToString()
# open('save pb file location & name','wb')
with open('./test9_company_data_cumsum.pb','wb') as f:
    f.write(serialized)
