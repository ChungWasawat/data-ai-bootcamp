import json
from commons.vertex_agent_search import vertex_search_retail_products
from commons.dialogflowcx_answer import detect_intent_text
from commons.flex_message_builder import build_flex_carousel_message

from linebot.v3.messaging import (
    ReplyMessageRequest,
    TextMessage,
    TemplateMessage,
    FlexMessage,
    Emoji,
    QuickReply,
    QuickReplyItem,
    CarouselTemplate,
    CarouselColumn,
    FlexContainer,
    MessageAction,
    URIAction,
    PostbackAction,
    CameraAction,
    CameraRollAction,
)



def handle_text_by_keyword(event, line_bot_api):
    text = event.message.text
    
    if text == "my_profile":
        profile = line_bot_api.get_profile(user_id=event.source.user_id)

        with open("templates/profile_bubble.json") as profile_bubble_json:
            profile_bubble = profile_bubble_json.read()
            profile_bubble = (
                profile_bubble.replace("USER_PROFILE_URL", profile.picture_url)
                .replace("LINE_USER_NAME", profile.display_name)
                .replace("LINE_USER_STATUS", profile.status_message)
            )
            flex_profile_message = FlexMessage(
                alt_text="my_profile_bubble",
                contents=FlexContainer.from_json(profile_bubble),
            )
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[
                        TextMessage(text="Profile JSON: " + profile.to_str()),
                        flex_profile_message,
                    ],
                )
            )
    
    elif text == "emojis":
        emojis = [
            Emoji(index=0, product_id="5ac1bfd5040ab15980c9b435", emoji_id="001"),
            Emoji(index=13, product_id="5ac1bfd5040ab15980c9b435", emoji_id="002"),
        ]
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text="$ LINE emoji $", emojis=emojis)],
            )
        )

    elif text == "emoji":
        emojis = [
            Emoji(index=0, product_id="5ac1bfd5040ab15980c9b435", emoji_id="004"),
            Emoji(index=13, product_id="5ac1bfd5040ab15980c9b435", emoji_id="005"),
        ]
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text="$ LINE emoji $", emojis=emojis)],
            )
        )
    
    elif text == "quota":
        quota = line_bot_api.get_message_quota()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[
                    TextMessage(text="type: " + quota.type),
                    TextMessage(text="value: " + str(quota.value)),
                ],
            )
        )
    
    elif text == "quota_consumption":
        quota_consumption = line_bot_api.get_message_quota_consumption()
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[
                    TextMessage(
                        text="total usage: " + str(quota_consumption.total_usage)
                    )
                ],
            )
        )
        
    elif text == "carousel":
        carousel_template = CarouselTemplate(
            columns=[
                CarouselColumn(
                    text="hoge1",
                    title="fuga1",
                    actions=[
                        URIAction(label="Go to line.me", uri="https://line.me"),
                        PostbackAction(label="ping", data="ping"),
                    ],
                ),
                CarouselColumn(
                    text="hoge2",
                    title="fuga2",
                    actions=[
                        PostbackAction(
                            label="ping with text", data="ping", text="ping"
                        ),
                        MessageAction(label="Translate Rice", text="米"),
                    ],
                ),
            ]
        )
        template_message = TemplateMessage(
            alt_text="Carousel alt text", template=carousel_template
        )
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token, messages=[template_message]
            )
        )
        
    elif text == "ค้นหาสินค้า":
        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[
                    TextMessage(
                        text="คุณสามารถพิมพ์ชื่อสินค้าที่ต้องการ หรือ ส่งรูปสินค้าที่ต้องการค้นหาได้",
                        quick_reply=QuickReply(
                            items=[
                                QuickReplyItem(action=CameraAction(label="ถ่ายรูปภาพ")),
                                QuickReplyItem(
                                    action=CameraRollAction(label="ส่งรูปภาพ")
                                ),
                            ]
                        ),
                    )
                ],
            )
        )

    elif text.startswith("#ค้นหา"):
        search_query = text[len("#ค้นหา") :].strip()
        response_dict = vertex_search_retail_products(search_query)
        build_flex_carousel_message(
            line_bot_api=line_bot_api,
            event=event,
            response_dict=response_dict,
            search_query=search_query,
        )
    
    else:
        response_text = detect_intent_text(text=text, session_id=event.source.user_id)

        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=response_text)],
            )
        )
