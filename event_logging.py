__author__ = 'nickroth'


class EventLogger(object):
    def __init__(self, env, level='HIGH', resolution=1.0):
        self.level = level
        self.resolution = resolution
        self.log_count = 0.0
        self.log_period = int(1.0/resolution)

    def log(self, msg):
        if self.level is 'HIGH' and self.log_count >= self.log_period:
            print(msg)
            self.log_count = 1.0
        else:
            self.log_count += 1


class Event(object):
    def __init__(self, time, *args):
        self.time = time
        self.args = args

    def __str__(self):
        the_event = [str(self.time), ':  ']
        the_args = [str(arg) for arg in self.args]
        the_event.extend(the_args)
        return " ".join(the_event)


if __name__ == '__main__':
    from toys import Toy
    ev = Event(10, Toy(10, 12), 'shot', Toy(10, 10))
    print(ev)