__author__ = 'nickroth'


class Population(object):

    population = 0

    def __init__(self):
        self.id = self.gen_id()

    def __str__(self):
        return self.__class__.__name__ + " " + str(self.id)

    @classmethod
    def gen_id(cls):
        """
        Generates a unique component id based on the current number of component instances that exist.

        :return: string id for the component
        """
        cls.population += 1
        return cls.population - 1