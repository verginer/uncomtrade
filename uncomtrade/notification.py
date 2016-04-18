# http://stackoverflow.com/questions/17651017/python-post-osx-notification

import os


# The notifier function
def notify(title, subtitle, message, sound, voice_allert=True):
    t = '-title {!r}'.format(title)
    s = '-subtitle {!r}'.format(subtitle)
    m = '-message {!r}'.format(message)
    sound = '-sound {!r}'.format(sound)
    os.system('terminal-notifier {}'.format(' '.join([m, t, s, sound])))
    if voice_allert:
        os.system("say 'Limit reached, please change proxy'")


