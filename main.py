from api import KafkaAPI
from PIL import Image

# initialize constants used to control image spatial dimensions and
# data type
IMAGE_WIDTH = 224
IMAGE_HEIGHT = 224
IMAGE_CHANS = 3
IMAGE_DTYPE = "float32"

NEW_TOPIC = "new-topic"

def main():
    ## SOME API FUNCTIONALITY ##
    # instantiate API
    kafkaApi = KafkaAPI()
    # create new topic
    kafkaApi.create_topic([NEW_TOPIC])
    # list topics on server
    kafkaApi.list_topics()
    # delete the topic
    kafkaApi.delete_topics(NEW_TOPIC)
    # list topics on server
    kafkaApi.list_topics()
    # recreate the new topic
    kafkaApi.create_topic([NEW_TOPIC])

    # Send a command to the model
    current_topic = NEW_TOPIC
    kafkaApi.send_command(current_topic, "Train")

    # Received the command on the model end
    message = kafkaApi.receive_command(current_topic)
    print("Received message: ", message)

    # Send a stream of images to the model to classify
    img = Image.open("img/img.jpg")
    kafkaApi.send_img(current_topic, img)

    # receive the img on the model end for processing
    image = kafkaApi.receive_img(current_topic, IMAGE_DTYPE, (1, IMAGE_HEIGHT, IMAGE_WIDTH, IMAGE_CHANS))
    image.show()


if __name__ == '__main__':
    main()