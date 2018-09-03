from uuid import uuid4
from collections import namedtuple
from pprint import pprint
from contextlib import contextmanager
import posixfile
import pickle
import os

NA = "__NA__"
Datom = namedtuple("Datom", "e a v t asserted retracted")
Db = namedtuple("Db", "datoms t")


def entity():
    return "ENTITY." + str(uuid4())[:8]


@contextmanager
def transact(conn):
    lock_file = posixfile.open(conn + ".lock", 'w')
    lock_file.lock('w|')
    db_old = _read(conn)
    db_new = Db(db_old.datoms, db_old.t + 1)
    try:
        yield db_new
    finally:
        with open(conn, 'wb') as f:
            pickle.dump(db_new, f)
        lock_file.lock('u')
        lock_file.close()


def _read(conn):
    if not os.path.exists(conn):
        return Db([], 0)
    else:
        with open(conn, 'rw') as f:
            return pickle.load(f)


@contextmanager
def read(conn):
    yield _read(conn)


def add(db, e, a, v):
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
        elif datom.retracted is True:
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
    CONN = "pytoymic.pickle"
    os.system("rm " + CONN)
    with transact(CONN) as db:
        user_entity = add(db, entity(), "user.id", NA)
        add(db, user_entity, "user.name", "Hans Jensen")
        add(db, user_entity, "user.email", "foo@gmail.com")
        user_entity2 = add(db, entity(), "user.id", NA)
        add(db, user_entity2, "user.name", "Jens Andersen")
        add(db, user_entity2, "user.email", "bar@gmail.com")

    with transact(CONN) as db:
        retract(db, user_entity, "user.email")

    with transact(CONN) as db:
        add(db, user_entity, "user.email", "bar@gmail.com")

    with transact(CONN) as db:
        message_entity = add(db, entity(), "message.id", NA)
        add(db, message_entity, "message.message", "Hej med dig")
        add(db, message_entity, "message.user_id", user_entity)
        message_entity = add(db, entity(), "message.id", NA)
        add(db, message_entity, "message.message", "Fedt mand")
        add(db, message_entity, "message.user_id", user_entity)
        message_entity2 = add(db, entity(), "message.id", NA)
        add(db, message_entity2, "message.message", "Yeah")
        add(db, message_entity2, "message.user_id", user_entity2)

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
