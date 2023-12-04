from paddleocr import PaddleOCR
import urllib.request

ocr = PaddleOCR(lang="korean")


def get_image_ocr(link):
    try:
        img_bytes = urllib.request.urlopen(link).read()
        result = ocr.ocr(img_bytes, cls=False)
        ocr_result = result[0]

        sentences = ""
        for item in ocr_result:
            sentences += f" {item[-1][0]}"
        return sentences
    except Exception as e:
        # TODO: Exception Handling
        print(e)
        return ""
