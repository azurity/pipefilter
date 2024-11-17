from pipefilter import pipe_filter, pipe_reducer, State
from pipefilter.ros import BagInfo, bag_pipe
from sensor_msgs.msg import Imu, MagneticField
import rospy
from math import sqrt
from contextlib import ExitStack


@pipe_filter
def extract_acc_value(bags):
    msg: Imu = bags['/imu/data']
    acc = msg.linear_acceleration
    return sqrt(acc.x ** 2 + acc.y ** 2 + acc.z ** 2)


@pipe_reducer
def avg(state, value):
    state[0] += 1
    state[1] += value
    return state[1] / state[0]


if __name__ == '__main__':
    rospy.init_node('debug', anonymous=True)
    with ExitStack() as ctx:
        state = State([0, 0])(ctx)
        datas = bag_pipe([
            BagInfo('test.bag', '/imu/data', Imu),
            BagInfo('test.bag', '/imu/mag', MagneticField),
        ])(ctx)
        value = extract_acc_value(datas)(ctx)
        avg_value = avg(state, value)(ctx)
        print(avg_value.value)
        print(next(avg_value))
