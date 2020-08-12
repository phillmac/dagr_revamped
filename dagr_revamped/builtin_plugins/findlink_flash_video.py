import pickle


def setup(manager):
    manager.register_findlink_b('flash_video', find_flash_video)
    return True


def find_flash_video(browser):
    current_page = browser.get_current_page()
    stage = current_page.find('div', {'data-hook': 'art_stage'})
    if stage:
        iframe_search = stage.find('iframe')
        if iframe_search:
            browser.open(iframe_search.attrs.get('src'))
            current_page = browser.get_current_page()
            embed_search = current_page.find('embed', {'id': 'sandboxembed'})
            if embed_search:
                return embed_search.attrs.get('src')
