import pickle
def setup(manager):
    manager.register_findlink_b('find_flash_video', find_flash_video)

def find_flash_video(browser):
    browser = pickle.loads(browser)
    current_page = browser.get_current_page()
    iframe_search = current_page.find('iframe', {'class': 'flashtime'})
    if iframe_search:
        browser.open(iframe_search.attrs.get('src'))
        current_page = browser.get_current_page()
        embed_search = current_page.find('embed', {'id': 'sandboxembed'})
        if embed_search:
            return embed_search.attrs.get('src')