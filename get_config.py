def get_config(config_name):
    from boto3 import Session
    session = Session()
    s3 = session.resource('s3')
    bucket = s3.Bucket("xax-configs1")
    all_objs = list(bucket.objects.all())
    objs = []
    for b in all_objs:
        objs.append(b.key)
    loc = objs.index(config_name)
    o = ast.literal_eval(all_objs[loc].get()['Body'].read().decode('utf-8'))
    return(o)
