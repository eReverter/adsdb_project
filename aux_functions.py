def connect_paths():
    """
    Connects all the zones
    """
    import os

    main = os.getcwd()
    landing_zone = os.path.join(main, 'landing_zone')
    temporal = os.path.join(landing_zone, 'temporal')
    persistent = os.path.join(landing_zone, 'persistent')
    formatted_zone = os.path.join(main, 'formatted_zone')
    trusted_zone = os.path.join(main, 'trusted_zone')
    exploitation_zone = os.path.join(main, 'exploitation_zone')
    analysis_path = os.path.join(main, 'analysis')

    return landing_zone, temporal, persistent, formatted_zone, trusted_zone, exploitation_zone, analysis_path