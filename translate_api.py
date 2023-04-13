from googletrans import Translator

translator = Translator(service_urls=['translate.googleapis.com'])

print(translator.translate('Hello, how are you?', dest='fr').text)