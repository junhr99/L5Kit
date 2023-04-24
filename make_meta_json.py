# In Google Colaboratory, you have to install module
# !pip install pymap3d

import numpy as np
import pymap3d as pm


def _undo_e7(value: float) -> float:
  return value / 1e7

x = np.cumsum(np.asarray(0) / 100)
y = np.cumsum(np.asarray(0) / 100)
z = np.cumsum(np.asarray(0) / 100)

# ego's reference point (field.reference.lat&lng in vehicle_state topic)
# latitude*(10^7)=1078.37301909, longtitude*(10^7)=-6692.45061389
frame_lat, frame_lng = _undo_e7(1078.37301909), _undo_e7(-6692.45061389)

xyz = np.stack(pm.enu2ecef(x, y, z, frame_lat, frame_lng, 0), axis=-1)

print(xyz)

'''
================= meta. json =================

{
    "world_to_ecef": [
        [ 
	  -0.5,
          -0.5,
          0.5,
	  xyz[0][0] 
	],
        [ 
	  0.5,
          -0.5,
          0.5, 
	  xyz[0][1]
	],
        [ 
	  -0.5,
          0.5,
          0.5,  
	  xyz[0][2]
	],
        [ 
	  0.0,
	  0.0, 
	  0.0,   
	  1.0
	] 
    ],

# You don't need
    "ecef_to_aerial": [
        [
            -7.17416495e-01,
            -1.14606296e+00,
            -1.62854453e+00,
            -5.72869824e+05
        ],
        [
            1.80065798e+00,
            -1.08914046e+00,
            -2.87877303e-02,
            3.00171963e+05
        ],
        [
            0.00000000e+00,
            0.00000000e+00,
            0.00000000e+00,
            0.00000000e+00
        ],
        [
            0.00000000e+00,
            0.00000000e+00,
            0.00000000e+00,
            1.00000000e+00
        ]
    ]
}

'''
