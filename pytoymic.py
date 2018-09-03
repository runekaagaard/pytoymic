from uuid import uuid4
from collections import namedtuple
from pprint import pprint
from contextlib import contextmanager
import posixfile
import pickle
import os

NA = "__NA__"
Datom = namedtuple("Datom", "e a v t asserted retracted")
CONN = "pytoymic.pickle"


def entity():
    return "ENTITY." + str(uuid4())[:8]


def touch(conn):
    if not os.path.exists(conn):
        with open(conn, 'wb') as f:
            pickle.dump(Db([], 0), f)


@contextmanager
def transact(conn):
    lock_file = posixfile.open(conn + ".lock", 'w')
    lock_file.lock('w|')
    with read(conn) as db:
        pass
    try:
        db.t += 1
        yield db
    finally:
        with open(conn, 'wb') as f:
            pickle.dump(db, f)
        lock_file.lock('u')
        lock_file.close()


@contextmanager
def read(conn):
    if not os.path.exists(conn):
        yield Db([], 0)
    else:
        with open(conn, 'rw') as f:
            yield pickle.load(f)


class Db(object):
    def __init__(self, datoms, t):
        self.datoms = datoms
        self.t = t

    def __repr__(self):
        return "DB @ {}".format(self.t)


def _assert(db, e, a, v):
    datom = Datom(e, a, v, db.t, True, False)
    db.datoms.append(datom)

    return datom.e


def retract(db, e, a):
    datom = Datom(e, a, NA, db.t, False, True)
    db.datoms.append(datom)

    return datom.e


def query(db, e=None, a=None, v=None, as_of=None):
    e_a_pairs = set()
    if as_of is None:
        as_of = db.t
    datoms = []
    for datom in reversed(db.datoms):
        if datom.t > as_of:
            continue
        elif e is not None and e != datom.e:
            continue
        elif a is not None and a != datom.a:
            continue
        elif v is not None and v != datom.v:
            continue
        else:
            e_a_pair = datom.e + datom.a
            if e_a_pair in e_a_pairs:
                continue
            datoms.append(datom)
            e_a_pairs.add(e_a_pair)

    return datoms


def pquery(db, *args, **kwargs):
    print "\nQUERY at", db.t
    print " " * 3, " ".join("{}={}".format(k, v)
                            for k, v in kwargs.iteritems())
    print "RESULTS"
    datoms = query(db, *args, **kwargs)
    for datom in datoms:
        print " " * 3, " ".join("{}={}".format(k, v)
                                for k, v in datom._asdict().iteritems())

    return datoms


if __name__ == "__main__":
    with transact(CONN) as db:
        user_entity = _assert(db, entity(), "user.id", NA)
        _assert(db, user_entity, "user.name", "Hans Jensen")
        _assert(db, user_entity, "user.email", "foo@gmail.com")
        user_entity2 = _assert(db, entity(), "user.id", NA)
        _assert(db, user_entity2, "user.name", "Jens Andersen")
        _assert(db, user_entity2, "user.email", "bar@gmail.com")

    with transact(CONN) as db:
        retract(db, user_entity, "user.email")

    with transact(CONN) as db:
        _assert(db, user_entity, "user.email", "bar@gmail.com")

    with transact(CONN) as db:
        message_entity = _assert(db, entity(), "message.id", NA)
        _assert(db, message_entity, "message.message", "Hej med dig")
        _assert(db, message_entity, "message.user_id", user_entity)
        message_entity = _assert(db, entity(), "message.id", NA)
        _assert(db, message_entity, "message.message", "Fedt mand")
        _assert(db, message_entity, "message.user_id", user_entity)
        message_entity2 = _assert(db, entity(), "message.id", NA)
        _assert(db, message_entity2, "message.message", "Yeah")
        _assert(db, message_entity2, "message.user_id", user_entity2)

        print ":...................."
        pprint(db.datoms)
        print "----------------------"

    with read(CONN) as db:
        pquery(db, a="user.email", v="bar@gmail.com")
        pquery(db, a="user.email", as_of=2)
        pquery(db, a="user.email", as_of=3)
        pquery(db, e=user_entity)
        datoms = pquery(db, a="message.user_id", v=user_entity)
        print "\nOUTPUT messages belonging to user", user_entity
        for datom in datoms:
            print "    ", query(db, e=datom.e, a="message.message")[0].v

        datoms = pquery(db, a="message.user_id", v=user_entity2)
        print "\nOUTPUT messages belonging to user", user_entity2
        for datom in datoms:
            print "    ", query(db, e=datom.e, a="message.message")[0].v
