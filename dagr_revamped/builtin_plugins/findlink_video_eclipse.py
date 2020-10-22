def setup(manager):
    manager.register_findlink('eclipse_video', find_video)
    return True

def find_video(current_page):
    screen_block = current_page.find('div', {'data-playable-hook': 'screen-block'})
    if screen_block:
        video = screen_block.find('video')
        if video:
            return video.attrs.get('src')