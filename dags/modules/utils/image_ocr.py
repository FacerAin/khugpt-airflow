import urllib.request

from paddleocr import PaddleOCR

# TODO: To select language mode.
ocr = PaddleOCR(lang="korean")


def get_image_ocr(link: str) -> str:
    """Get text from image link using OCR.

    Args:
        link: A link of image.

    Returns:
        The sentences of image.
    """

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
