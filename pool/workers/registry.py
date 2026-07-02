"""Worker registry rules used by Master 3.0.

Important invariant: worker_id is the technical identity, worker_name is the
stable display/accounting name. A connected worker_name may exist only once;
reconnects are allowed only for the same worker_id.
"""
