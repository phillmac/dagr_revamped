class DAGRConfig():
    DEFAULTS = {
        'DeviantArt': {
            'BaseUrl': 'https://www.deviantart.com',
            'MatureContent': False,
        },
        'DeviantArt.Modes.Album':{
            'url_fmt': '{base_url}/{deviant_lower}/gallery/{mval}?offset={offset}'
        },
        'DeviantArt.Modes.Category':{
            'url_fmt': '{base_url}/{deviant_lower}/gallery/?catpath={mval}&offset={offset}'
        },
        'DeviantArt.Modes.Collection':{
            'url_fmt': '{base_url}/{deviant_lower}/favourites/{mval}?offset={offset}'
        },
        'DeviantArt.Modes.Query':{
            'url_fmt': '{base_url}/{deviant_lower}/gallery/?q={mval}&offset={offset}'
        },
        'DeviantArt.Modes.Scraps':{
            'url_fmt': '{base_url}/{deviant_lower}/gallery/?catpath=scraps&offset={offset}'
        },
        'DeviantArt.Modes.Favs':{
            'url_fmt': '{base_url}/{deviant_lower}/favourites/?catpath=/&offset={offset}',
            'group_url_fmt': '{base_url}/{deviant_lower}/favourites/?offset={offset}'
        },
        'DeviantArt.Modes.Gallery':{
            'url_fmt': '{base_url}/{deviant_lower}/gallery/?catpath=/&offset={offset}',
            'group_url_fmt': '{base_url}/{deviant_lower}/gallery?offset={offset}'
        },
        'DeviantArt.Modes.Search':{
            'url_fmt': '{base_url}?q={mval}&offset={offset}'
        },
        'DeviantArt.Offset':{
            'Folders': 10,
            'Pages': 24
        },
        'Dagr': {
            'OutputDirectory': '~/dagr',
            'Overwrite': False,
            'SaveProgress': 50,
            'Verbose': False,
        },
        'Dagr.Cache': {
            'Artists': '.artists',
            'FileNames': '.filenames',
            'DownloadedPages': '.dagr_downloaded_pages'
        },
        'DAGR.RetryEexceptionNames': {
            'OSError': True,
            'ChunkedEncodingError': True,
            'ConnectionError': True
        }
    }