import requests
import json 

def _flatten_campaigns(campaigns: list):
    """Return a flattened list of dicts for each campaign in a list of campaign objects, one record per campaign"""
    
    campaigns_flattened = []
    
    for campaign in campaigns:
    
        campaigns_flattened.append({   
                "campaign_id": campaign.get("id"),
    
                "name": campaign.get('attributes',{}).get('name'),
                "status": campaign.get('attributes',{}).get('status'),
                "archived": campaign.get('attributes',{}).get('archived'),
                "included_audience": campaign.get('attributes',{}).get('audiences',{}).get('included',[]),
                "excluded_audience": campaign.get('attributes',{}).get('audiences',{}).get('excluded',[]),
                "created_at": campaign.get('attributes',{}).get("created_at"),
                "scheduled_at": campaign.get('attributes',{}).get("scheduled_at"),
                "send_time": campaign.get('attributes',{}).get("send_time"),
                "updated_at": campaign.get('attributes',{}).get("updated_at")
            })
    
        return campaigns_flattened

def list_all_campaigns(request_headers: dict):
    """List all campaigns in Klayvio"""

    print(f'Fetching all campaigns')
    
    url = "https://a.klaviyo.com/api/campaigns?filter=and(equals(messages.channel,'email'))"

    r = requests.get(url, headers=request_headers)
    r_json = json.loads(r.text)

    all_data = _flatten_campaigns(r_json.get('data',{}))
    
    while r_json.get('links',{}).get('next',None):
        print(r_json.get('links',{}).get('self',{}))

        r = requests.get(r_json.get('links',{}).get('next',None), headers=request_headers)
        r_json = json.loads(r.text)
        all_data.extend(_flatten_campaigns(r_json.get('data',{})))
        print(f'{len(all_data)} records retrieved')    
        return all_data


def _flatten_message(message: dict):


  sent_messages = []

  for sent in message.get('attributes',{}).get("send_times"):
      sent_message = {   
          "message_id": message.get("id"),
          "channel": message.get('attributes',{}).get('definition',{}).get("channel"),
          "label": message.get('attributes',{}).get('definition',{}).get("label"),
          "subject": message.get('attributes',{}).get('definition',{}).get('content',{}).get("subject"),
          "preview_text": message.get('attributes',{}).get('definition',{}).get('content',{}).get("preview_text"),
          "from_email": message.get('attributes',{}).get('definition',{}).get('content',{}).get("from_email"),
          "from_label": message.get('attributes',{}).get('definition',{}).get('content',{}).get("from_label"),
          "reply_to_email": message.get('attributes',{}).get('definition',{}).get('content',{}).get("reply_to_email"),
          "cc_email": message.get('attributes',{}).get('definition',{}).get('content',{}).get("cc_email"),
          "bcc_email": message.get('attributes',{}).get('definition',{}).get('content',{}).get("bcc_email"),
          "send_time": sent.get("datetime"),
          "created_at": message.get('attributes',{}).get("created_at"),
          "updated_at": message.get('attributes',{}).get("updated_at")
      }

      sent_messages.append(sent_message)

  return sent_messages

def _get_message_html_template(campaign_message_id: str, request_headers: dict):
    """Return the html template used for a given campaign message"""

    
    print(f'Fetching campaign message template for campaign-message {campaign_message_id}')
    
    url = f'https://a.klaviyo.com/api/campaign-messages/{campaign_message_id}/template'
    
    r = requests.get(url, headers=request_headers)
    r_json = json.loads(r.text)
    print(r_json)
    return r_json.get('data',{}).get('attributes',{}).get('html') if r_json['data'] else None

def _get_message_details(campaign_message_id: str, request_headers: dict):
    """Return a flattened list of dicts for each message sent in a klaviyo campaign, one record per sent message"""
    
    print(f'Fetching messages for campaign-message {campaign_message_id}')
    url = f'https://a.klaviyo.com/api/campaign-messages/{campaign_message_id}'
    
    r = requests.get(url, headers=request_headers)
    r_json = json.loads(r.text)
    
    all_data = _flatten_message(r_json['data'])
    while r_json.get('links',{}).get('next',None):
      print(r_json.get('links',{}).get('self',{}))
    
      r = requests.get(r_json.get('links',{}).get('next',None), headers=request_headers)
      if r.status_code != 200:
          print(f'waiting.. response: {r.status_code}')
          time.sleep(5)
          break
      r_json = json.loads(r.text)
      all_data.append(_flatten_message(r_json['data']))
      print(len(all_data))
    
    return all_data


def _get_campaign_est_recipients(campaign_id, request_headers):
    """ Return estimated recipient count for a given campaign"""

    
    print(f'Fetching messages for campaign-message {campaign_id}')
    url = f'https://a.klaviyo.com/api/campaign-recipient-estimations/{campaign_id}'
    
    r = requests.get(url, headers=request_headers)
    r_json = json.loads(r.text)
    
    return r_json.get('data',{}).get('attributes',{}).get('estimated_recipient_count',0)
    
    
