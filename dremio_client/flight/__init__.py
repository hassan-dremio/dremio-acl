# -*- coding: utf-8 -*-

try:
    import pyarrow as pa
    from pyarrow import flight
    from .flight_auth import HttpDremioClientAuthHandler
    from pyarrow.compat import tobytes

    def connect(
        hostname="localhost", port=47470, username="dremio", password="dremio123", tls_root_certs_filename=None
    ):
        """
        Connect to and authenticate against Dremio's arrow flight server. Auth is skipped if username is None

        :param hostname: Dremio coordinator hostname
        :param port: Dremio coordinator port
        :param username: Username on Dremio
        :param password: Password on Dremio
        :param tls_root_certs_filename: use ssl to connect with root certs from filename
        :return: arrow flight client
        """
        if tls_root_certs_filename:
            with open(tls_root_certs_filename) as f:
                tls_root_certs = f.read()
            location = "grpc+tls://{}:{}".format(hostname, port)
            c = flight.FlightClient(location, tls_root_certs=tls_root_certs)
        else:
            location = "grpc+tcp://{}:{}".format(hostname, port)
            c = flight.FlightClient(location)
        if username:
            c.authenticate(HttpDremioClientAuthHandler(username, password if password else ""))
        return c

    def query(
        sql,
        client=None,
        hostname="localhost",
        port=47470,
        username="dremio",
        password="dremio123",
        pandas=True,
        tls_root_certs_filename=False,
    ):
        """
        Run an sql query against Dremio and return a pandas dataframe or arrow table

        Either host,port,user,pass tuple or a pre-connected client should be supplied. Not both

        :param sql: sql query to execute on dremio
        :param client: pre-connected client (optional)
        :param hostname: Dremio coordinator hostname (optional)
        :param port: Dremio coordinator port (optional)
        :param username: Username on Dremio (optional)
        :param password: Password on Dremio (optional)
        :param pandas: return a pandas dataframe (default) or an arrow table
        :param tls_root_certs_filename: use ssl to connect with root certs from filename
        :return:
        """
        if not client:
            client = connect(hostname, port, username, password, tls_root_certs_filename)

        info = client.get_flight_info(flight.FlightDescriptor.for_command(tobytes(sql)))
        reader = client.do_get(info.endpoints[0].ticket)
        batches = []
        while True:
            try:
                batch, _ = reader.read_chunk()
                batches.append(batch)
            except StopIteration:
                break
        data = pa.Table.from_batches(batches)
        if pandas:
            return data.to_pandas()
        else:
            return data


except ImportError:

    def connect(*args, **kwargs):
        raise NotImplementedError("Python Flight bindings require Python 3 and pyarrow > 0.14.0")

    def query(*args, **kwargs):
        raise NotImplementedError("Python Flight bindings require Python 3 and pyarrow > 0.14.0")
