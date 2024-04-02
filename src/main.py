__author__ = 'Stiander'

import datetime , os
from concurrent import futures
import logging
import grpc
import vedbjorn_pb2
import vedbjorn_pb2_grpc
from fpdf import FPDF
import pytz
from PIL import Image
import tempfile
from bson.objectid import ObjectId
import math
import requests, random
from google.auth import jwt

from libs.commonlib.defs import MAX_ALLOWED_REQUEST_ITEMS , MAX_ALLOWED_RESERVATION_WEEKS, IS_FAKE , PRICE_PER_WEEK_RESERVATION_PER_BAG
from libs.commonlib.db_insist import set_graph_changed , get_db
from libs.commonlib.pymongo_paginated_cursor import PaginatedCursor as mpcur
from libs.commonlib.location_funcs import location_info , make_coordinate_name , coordinates_from_address
from libs.commonlib.graph_funcs import loc_to_graph , get_location_with_name , get_user_with_email, user_to_location, \
    delete_and_detach, buyrequest_to_user, sellrequest_to_user, driverequest_to_user, get_buyrequests_with_email, \
    get_sellrequests_with_email, get_driverequests_with_email , set_reserved_weeks_BuyRequest , set_current_requirement_BuyRequest, \
    remove_buyRequest , update_current_capacity_sellRequest , remove_sellRequest , update_available_driveRequest , \
    remove_driveRequest, get_sell_requests_in_muni, get_buy_requests_in_muni, get_drive_requests_in_muni , \
    get_sell_requests_in_county, get_buy_requests_in_county, get_drive_requests_in_county , get_all_countys , \
    get_all_municipalities , set_driver_available , set_driver_available_again_time , get_driver_location , \
    get_drivers_in_county , get_user_with_driverequest_name, get_user_with_sellrequest_name , get_user_location , \
    get_user_with_phone , get_buyrequests_with_phone , get_sellrequests_with_phone, get_driverequests_with_phone , \
    get_all_drivers_in_county, get_all_sellers_in_county, get_all_buyers_in_county, get_buyrequests

from libs.matchlib.actions import claim_planned_route , decline_planned_route , handle_pickup , handle_delivery , \
    handle_return , verify_that_route_is_completed , CHEATING__set_all_delivery_payments_to_completed , \
    wrap_up_ongoing_route

DRIVER_QUARANTINE_TIME  = int(os.getenv('DRIVER_QUARANTINE_TIME' , 60))
DEBUGGING = os.getenv('DEBUGGING' , '') == 'true'

def check_db() :
    get_db()
check_db()

def hard_bool(_obj, _keyword : str = '') -> bool :
    if isinstance(_obj, bool) :
        return _obj
    if _keyword == '' :
        try :
            conv : bool = bool(_obj)
            return conv
        except Exception :
            return False
    if not _obj or not _keyword:
        return False
    _lowkey = _keyword.lower()
    _midkey = str(_keyword[0]).upper() + _lowkey[1:len(_lowkey)]
    _topkey = _keyword.upper()
    fake_val = _obj.get(_lowkey , _obj.get(_midkey, _obj.get(_topkey, False)))
    if isinstance(fake_val, bool) :
        return fake_val
    elif isinstance(fake_val, str) :
        return fake_val.lower() == 'true'
    else :
        try :
            return bool(fake_val)
        except Exception :
            return False

def is_fake(_obj : dict) :
    return hard_bool(_obj, 'fake')

def dict_to_Sellrequest(obj : dict) -> vedbjorn_pb2.SellRequest :
    if not obj :
        return vedbjorn_pb2.SellRequest(
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = 'Doesnt exist',
                code = 404,
                ok = False
            )
        )
    return vedbjorn_pb2.SellRequest(
        contact_info = vedbjorn_pb2.UserContactInfo(
            email_address = str(obj.get('email' , obj.get('email_address' , 'N/A'))) ,
            phone_number  = str(obj.get('phone' , obj.get('phone_number' , 'N/A')))
        ) ,
        name               = str(obj.get('name' , '')) ,
        current_capacity   = int(obj.get('current_capacity' , 0)) ,
        amount_reserved    = int(obj.get('amount_reserved' , 0)) ,
        amount_staged      = int(obj.get('amount_staged' , 0)) ,
        num_reserved       = int(obj.get('num_reserved' , 0)) ,
        num_staged         = int(obj.get('num_staged' , 0)) ,
        prepare_for_pickup = int(obj.get('prepare_for_pickup' , 0)) ,
        fake               = is_fake(obj)
    )

def dict_to_Driverequest(obj : dict) -> vedbjorn_pb2.DriveRequest :
    if not obj :
        return vedbjorn_pb2.DriveRequest(
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = 'Doesnt exist',
                code = 404,
                ok = False
            )
        )
    return vedbjorn_pb2.DriveRequest(
        contact_info=vedbjorn_pb2.UserContactInfo(
            email_address = str(obj.get('email', obj.get('email_address', 'N/A'))),
            phone_number  = str(obj.get('phone', obj.get('phone_number', 'N/A')))
        ),
        name                 = str(obj.get('name', '')),
        available            = hard_bool(obj.get('available' , False)) ,
        num_staged_pickups   = int(obj.get('num_staged_pickups' , 0)) ,
        available_again_time = float(obj.get('available_again_time' , 0)) ,
        fake                 = is_fake(obj)
    )

def dict_to_User(obj : dict) -> vedbjorn_pb2.User :
    if not obj :
        return vedbjorn_pb2.User(
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = 'Doesnt exist',
                code = 404,
                ok = False
            )
        )
    return vedbjorn_pb2.User(
        contact_info = vedbjorn_pb2.UserContactInfo(
            email_address = str(obj.get('email', obj.get('email_address', 'N/A'))),
            phone_number  = str(obj.get('phone', obj.get('phone_number', 'N/A')))
        ),
        location_name = str(obj.get('location_name' , '')) ,
        firstname     = str(obj.get('firstname' , '')) ,
        lastname      = str(obj.get('lastname' , ''))
    )

def dict_to_Buyrequest(obj : dict) -> vedbjorn_pb2.BuyRequest :
    if not obj :
        return vedbjorn_pb2.BuyRequest(
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = 'Doesnt exist',
                code = 404,
                ok = False
            )
        )
    return vedbjorn_pb2.BuyRequest(
        contact_info=vedbjorn_pb2.UserContactInfo(
            email_address=obj.get('email', obj.get('email_address', 'N/A')),
            phone_number=obj.get('phone', obj.get('phone_number', 'N/A'))
        ),
        name                = str(obj.get('name', '')),
        current_requirement = int(obj.get('current_requirement' , 0)) ,
        reserved_weeks      = int(obj.get('reserved_weeks', -1)) ,
        reserve_target      = str(obj.get('reserve_target' , '')) ,
        last_calced         = int(obj.get('last_calced' , 0)) ,
        claimed_by_driver   = hard_bool(obj, 'claimed_by_driver') ,
        fake                = is_fake(obj)
    )

def dict_to_Location(loc : dict) -> vedbjorn_pb2.Location:
    return vedbjorn_pb2.Location(
        lat          = float(loc['lat']),
        lng          = float(loc['lng']),
        place_id     = int(loc['place_id']),
        osm_type     = str(loc['osm_type']),
        display_name = str(loc['display_name']),
        road         = str(loc['road']),
        quarter      = str(loc['quarter']),
        village      = str(loc['village']),
        farm         = str(loc['farm']),
        municipality = str(loc['municipality']),
        county       = str(loc['county']),
        country      = str(loc['country']),
        postcode     = str(loc['postcode']),
        osm_id       = int(loc.get('osm_id' , -1)) ,
        lclass       = str(loc.get('class' , '')),
        ltype        = str(loc.get('type' , '')),
        importance   = float(loc.get('importance' , -1)) ,
        name         = str(loc['name']) ,
        info = vedbjorn_pb2.GeneralInfoMessage(
            content = str(loc.get('info' , {}).get('content' , '')) ,
            code    = int(loc.get('info' , {}).get('code' , 0)) ,
            ok      = hard_bool(loc.get('info' , {}).get('ok' , True))
        )
    )

def User_to_dict(user : vedbjorn_pb2.User) -> dict :
    return {
        'email' : user.contact_info.email_address ,
        'phone' : user.contact_info.phone_number ,
        'name' : user.firstname + ' ' + user.lastname,
        'firstname' : user.firstname,
        'lastname' : user.lastname,
        'location_name' : user.location_name,
        'fake' : IS_FAKE
    }


class VedbjornServer(vedbjorn_pb2_grpc.VedbjornFunctionsServicer) :

    def extract_auth_header(self, context):
        """

        :param context:
        :return:
        """
        metadata = context.invocation_metadata()
        for meta in metadata :
            if meta.key == 'authorization' :
                return meta.value
        return ''

    def let_the_client_in(self, context):
        """

        :param context:
        :return:
        """
        principal = os.getenv('PRINCIPAL', '')
        if not principal :
            return DEBUGGING
        auth_header = self.extract_auth_header(context)
        if auth_header:
            auth_type, creds = auth_header.split(" ", 1)
            if auth_type.lower() == "bearer":
                claims = jwt.decode(creds, verify=False)
                client_is_principal = claims.get('email', '_') == principal
                return client_is_principal
        return False

    def ShortCountryInfo(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """
        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.Counties(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        counties = get_all_countys()
        ret = vedbjorn_pb2.Counties()
        for county in counties:
            if not county or len(county) <= 0:
                continue
            countyName = county[0].get('name', '')
            cnty = vedbjorn_pb2.County(
                name = vedbjorn_pb2.Name(value = countyName)
            )
            munis = get_all_municipalities(countyName)
            for muni in munis:
                if not muni or len(muni) <= 0:
                    continue
                muniName = muni[0].get('name', '')
                cnty.municipalities.append(muniName)
            ret.counties.append(cnty)
        return ret

    def CoordinateToLocation(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.Location(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        locname = make_coordinate_name(request.lat , request.lng)
        already_loc = get_location_with_name(locname)
        if already_loc:
            return dict_to_Location(already_loc[0][0])
        loc = location_info(request.lat , request.lng)
        return dict_to_Location(loc)

    def NameToLocation(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.Location(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        loc = get_location_with_name(request.content)
        if loc:
            return dict_to_Location(loc[0][0])
        return vedbjorn_pb2.Location(
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = 'Not found' ,
                code = 404 ,
                ok = False
            )
        )

    def LocationToGraph(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.Location(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        locname = make_coordinate_name(request.lat , request.lng)
        already_loc = get_location_with_name(locname)
        if not already_loc :
            loc = location_info(request.lat , request.lng)
            loc_to_graph(loc, {'fake': IS_FAKE}, {'fake': IS_FAKE})
            return dict_to_Location(loc)
        else:
            return dict_to_Location(already_loc[0][0])

    def FindCoordinatesInAddress(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.Location(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        loc = coordinates_from_address(request.road, request.postcode, request.municipality, request.county, request.country)
        if not loc :
            return vedbjorn_pb2.Location(
                info = vedbjorn_pb2.GeneralInfoMessage(
                    content = 'Failed at geocoding' ,
                    code = 400 ,
                    ok = False
                )
            )
        return vedbjorn_pb2.Location(
            lat          = float(loc.get('lat' , -1)) ,
            lng          = float(loc.get('lng' , -1)) ,
            place_id     = int(loc.get('place_id', -1)),
            osm_type     = str(loc.get('osm_type', '')),
            osm_id       = int(loc.get('osm_id', -1)),
            display_name = str(loc.get('display_name', '')),
            lclass       = str(loc.get('class', '')),
            ltype        = str(loc.get('type', '')),
            importance   = float(loc.get('importance', -1)),
            road         = str(loc.get('road', request.road)),
            quarter      = str(request.quarter),
            village      = str(request.village),
            farm         = str(request.farm),
            municipality = str(loc.get('municipality', request.municipality)),
            county       = str(loc.get('county', request.county)),
            country      = str(loc.get('country', request.country)),
            postcode     = str(loc.get('postcode', request.postcode)),
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = '',
                code    = 200,
                ok      = True
            )
        )


    def GetUser(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.User(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        user = None
        if request.email_address :
            user = get_user_with_email(request.email_address)
        if not user :
            user = get_user_with_phone(request.phone_number)
        if not user:
            return vedbjorn_pb2.User(
                location_name = '' ,
                firstname = '' ,
                lastname = '' ,
                contact_info = vedbjorn_pb2.UserContactInfo(email_address = '', phone_number = '') ,
                fake = IS_FAKE ,
                info = vedbjorn_pb2.GeneralInfoMessage(
                    content = 'User doesnt exist' ,
                    code = 404 ,
                    ok = False
                )
            )
        else :
            userObj = user[0][0]
            mongodb = get_db()
            adm_doc = mongodb.insist_on_find_one_q('admins', {'email' : str(userObj['email']) })
            is_admin = adm_doc != None and adm_doc != {}
            return vedbjorn_pb2.User(
                location_name=userObj['location_name'],
                firstname=userObj['firstname'],
                lastname=userObj['lastname'],
                contact_info = vedbjorn_pb2.UserContactInfo(
                    email_address = str(userObj['email']) ,
                    phone_number = str(userObj.get('phone' , userObj.get('phone_number', '')))
                ) ,
                fake=is_fake(userObj) ,
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=200,
                    ok=True
                ) ,
                is_admin = is_admin
            )

    """
        rpc VerifyUserEmailStart (UserContactInfo) returns (GeneralInfoMessage) {}
        rpc VerifyUserEmail(EmailVerificationCode) returns (GeneralInfoMessage) {}
    """
    def VerifyUserEmailStart(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        def make_code() :
            list1 = [1, 2, 3, 4, 5, 6, 7, 8, 9]
            return str(random.choice(list1)) + str(random.choice(list1)) + str(random.choice(list1)) + str(random.choice(list1))

        email = request.email_address
        db = get_db()
        veri_doc = db.insist_on_find_one_q('notifications' , {'email' : email})
        if veri_doc and veri_doc.get('verified', False) == True :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Already verified',
                code=200,
                ok=True
            )
        elif veri_doc :
            new_code = make_code()
            db.insist_on_update_one(veri_doc, 'notifications', 'code', new_code)
            db.insist_on_update_one(veri_doc, 'notifications', 'timestamp', datetime.datetime.utcnow().timestamp())
            db.insist_on_update_one(veri_doc, 'notifications', 'text', 'Dette er koden for å bekrefte din epost : ' + new_code)
            db.insist_on_update_one(veri_doc, 'notifications', 'status', 'new')
        else :
            new_code = make_code()
            db.insist_on_insert_one('notifications', {
                'code' : new_code,
                'timestamp' : datetime.datetime.utcnow().timestamp(),
                'text' : 'Dette er koden for å bekrefte din epost : ' + new_code,
                'status' : 'new',
                'email' : email,
                'contentType' : 'verify email'
            })

        return vedbjorn_pb2.GeneralInfoMessage(
            content='Verification process initiated',
            code=200,
            ok=True
        )

    def VerifyUserEmail(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        email = request.email_address
        code  = request.code
        db = get_db()
        veri_doc = db.insist_on_find_one_q('notifications', {'email': email})
        if veri_doc and veri_doc.get('verified', False) == True:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Already verified',
                code=200,
                ok=True
            )
        elif veri_doc:
            ok = veri_doc.get('code', '_') == code
            if not ok :
                return vedbjorn_pb2.GeneralInfoMessage(
                    content='Mismatch',
                    code=400,
                    ok=False
                )
            _now = datetime.datetime.utcnow().timestamp()
            if _now - veri_doc['timestamp'] > 3600 :
                return vedbjorn_pb2.GeneralInfoMessage(
                    content='Expired',
                    code=400,
                    ok=False
                )
            db.insist_on_update_one(veri_doc, 'notifications', 'verified', True)
            db.insist_on_remove_attribute(veri_doc['_id'] , 'notifications', 'code')
            db.insist_on_insert_one('notifications', {
                'timestamp': datetime.datetime.utcnow().timestamp(),
                'text': 'Din epost har blitt bekreftet! :) Velkommen til Vedbjørn',
                'status': 'new',
                'email': email,
                'contentType': 'email verified'
            })

            return vedbjorn_pb2.GeneralInfoMessage(
                content='Match',
                code=200,
                ok=True
            )

        else:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Must initialize first',
                code=400,
                ok=False
            )

    def DeleteUser(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        user = get_user_with_email(request.email_address)
        if not user :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='User did not exist',
                code=400,
                ok=False
            )
        delete_and_detach('User', {'email': request.email_address})

        # TODO : When the graph has been manipulated, any staged graph analytic
        #        result must be rendered invalid, and new results must be
        #        calculated.
        set_graph_changed()

        return vedbjorn_pb2.GeneralInfoMessage(
            content = 'User was deleted' ,
            code    = 200,
            ok      = True
        )

    def CreateUser(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        if not request.contact_info.email_address :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Pleaser provide a valid email address',
                code=400,
                ok=False
            )
        if not request.contact_info.phone_number :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Pleaser provide a valid phone number',
                code=400,
                ok=False
            )
        already_user = get_user_with_email(request.contact_info.email_address)
        if already_user :
            return vedbjorn_pb2.GeneralInfoMessage(
                content = 'User already exists' ,
                code    = 400,
                ok      = False
            )
        already_loc = get_location_with_name(request.location_name)
        if not already_loc:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='That location was not found. Consider inserting it first',
                code=400,
                ok=False
            )
        user_to_location(request.location_name, User_to_dict(request), {'fake': IS_FAKE})
        return vedbjorn_pb2.GeneralInfoMessage(
            content='User ' + request.contact_info.email_address + ' inserted at ' + request.location_name,
            code=200,
            ok=True
        )

    def BuyRequestToUser(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        already_user = get_user_with_email(request.contact_info.email_address)
        if not already_user :
            return vedbjorn_pb2.GeneralInfoMessage(
                content = 'User doesnt exists' ,
                code    = 400,
                ok      = False
            )

        already_user_obj = already_user[0][0]

        already_sellreq = get_sellrequests_with_email(request.contact_info.email_address)
        if already_sellreq :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='buyrequest can not be created, because a sellrequest already exist for this user',
                code=403,
                ok=True
            )

        already_buyreq = get_buyrequests_with_email(request.contact_info.email_address)
        buyreq_name = 'BUY_' + already_user_obj['name']
        if not already_buyreq :
            buyrequest_to_user(
                buyrequest = {
                    'name' : buyreq_name ,
                    'current_requirement' : request.current_requirement ,
                    'reserved_weeks' : request.reserved_weeks ,
                    'reserve_target' : '' ,
                    'last_calced' : 0 ,
                    'claimed_by_driver' : False ,
                    'fake' : IS_FAKE
                } ,
                user = {
                    'email' : already_user_obj['email'],
                    'phone' : already_user_obj['phone']
                },
                relationship_meta = {
                    'fake' : IS_FAKE
                })

            # TODO : When the graph has been manipulated, any staged graph analytic
            #        result must be rendered invalid, and new results must be
            #        calculated.
            set_graph_changed()

            return vedbjorn_pb2.GeneralInfoMessage(
                content='buyrequest was created',
                code=200,
                ok=True
            )
        else:
            buyreqObj = already_buyreq[0][0]
            if request.current_requirement == buyreqObj['current_requirement'] and request.reserved_weeks == buyreqObj['reserved_weeks']:
                return vedbjorn_pb2.GeneralInfoMessage(
                    content='No changes made',
                    code=400,
                    ok=True
                )
            if request.current_requirement > MAX_ALLOWED_REQUEST_ITEMS :
                return vedbjorn_pb2.GeneralInfoMessage(
                    content='Maximum allowed bags (' + str(MAX_ALLOWED_REQUEST_ITEMS) + ') exceeded,',
                    code=400,
                    ok=True
                )
            if request.reserved_weeks > MAX_ALLOWED_RESERVATION_WEEKS :
                return vedbjorn_pb2.GeneralInfoMessage(
                    content='Maximum allowed reservation-weeks (' + str(MAX_ALLOWED_RESERVATION_WEEKS) + ') exceeded,',
                    code=400,
                    ok=True
                )
            if request.current_requirement != buyreqObj['current_requirement'] :
                if request.current_requirement <= 0:
                    return vedbjorn_pb2.GeneralInfoMessage(
                        content='Negative requirement : consider instead to delete the buyrequest',
                        code=400,
                        ok=True
                    )
                set_current_requirement_BuyRequest(buyreq_name, request.current_requirement)
            if request.reserved_weeks != buyreqObj['reserved_weeks'] :
                set_reserved_weeks_BuyRequest(buyreq_name, request.reserved_weeks)

                # TODO : When the graph has been manipulated, any staged graph analytic
                #        result must be rendered invalid, and new results must be
                #        calculated.
                set_graph_changed()

            return vedbjorn_pb2.GeneralInfoMessage(
                content='Ok, changes were made',
                code=200,
                ok=True
            )
    #rpc GetBuyRequestMatch(UserContactInfo) returns (GeneralInfoMessage) {}
    def GetBuyRequestMatch(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        already_user = get_user_with_email(request.email_address)
        if not already_user:
            return vedbjorn_pb2.GeneralInfoMessage(
                    content='User doesnt exist',
                    code=404,
                    ok=False
                )
        db = get_db()
        notifications = db.insist_on_find('notifications' , {
            'email' : request.email_address ,
            'contentType' : 'delivery'
        })
        for notification in mpcur(notifications) :
            if not notification :
                continue

            delivery = db.insist_on_find_one('deliveries' , notification.get('ref_id', None))
            if not delivery :
                continue

            payment = db.insist_on_find_one('vipps_payments_in', delivery.get('payment_ref' , None))
            if not payment :
                continue

            if payment.get('status' , '') == 'unpaid' :
                if 'ongoing_routes' in notification :
                    return vedbjorn_pb2.GeneralInfoMessage(
                            content=str(notification['ongoing_routes']),
                            code=200,
                            ok=True
                        )
                elif 'ongoing_route' in delivery.get('meta' , {}) :
                    return vedbjorn_pb2.GeneralInfoMessage(
                        content=str(delivery['meta']['ongoing_route']),
                        code=200,
                        ok=True
                    )
                else :
                    raise Exception("Unable to find onoging_route for a notification (buyer)")

        return vedbjorn_pb2.GeneralInfoMessage(
            content='',
            code=200,
            ok=True
        )

    def GetBuyRequest(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.BuyRequest(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        already_user = get_user_with_email(request.email_address)
        if not already_user:
            return vedbjorn_pb2.BuyRequest(
                contact_info=vedbjorn_pb2.UserContactInfo(email_address='', phone_number=''),
                name='',
                current_requirement=0,
                reserved_weeks=0,
                reserve_target='',
                last_calced=0,
                claimed_by_driver=False,
                fake=True,
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='User doesnt exist',
                    code=404,
                    ok=False
                )
            )

        already_user_obj = already_user[0][0]

        buyreq = get_buyrequests_with_email(request.email_address)
        if not buyreq :
            return vedbjorn_pb2.BuyRequest(
                contact_info = vedbjorn_pb2.UserContactInfo(email_address = '', phone_number='') ,
                name = '',
                current_requirement = 0 ,
                reserved_weeks = 0 ,
                reserve_target = '' ,
                last_calced = 0 ,
                claimed_by_driver = False ,
                fake = True,
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='Buyrequest doesnt exist',
                    code=404,
                    ok=False
                )
            )

        buyreqObj = buyreq[0][0]

        contact_info = vedbjorn_pb2.UserContactInfo(
                email_address = str(already_user_obj['email']) ,
                phone_number = str(already_user_obj['phone']))

        info = vedbjorn_pb2.GeneralInfoMessage(
                content='Buyrequest found',
                code=200,
                ok=True
            )

        ret = vedbjorn_pb2.BuyRequest(
            contact_info        = contact_info ,
            name                = str(buyreqObj['name']) ,
            current_requirement = int(buyreqObj['current_requirement']) ,
            reserved_weeks      = int(buyreqObj['reserved_weeks']) ,
            reserve_target      = str(buyreqObj['reserve_target']) ,
            last_calced         = int(buyreqObj['last_calced']) ,
            claimed_by_driver   = hard_bool(buyreqObj, 'claimed_by_driver') ,
            fake                = is_fake(buyreqObj),
            info                = info
        )
        return ret

    def DeleteBuyRequest(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        already_user = get_user_with_email(request.email_address)
        if not already_user:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='User doesnt exists',
                code=400,
                ok=False
            )

        already_user_obj = already_user[0][0]

        already_buyreq = get_buyrequests_with_email(request.email_address)
        if not already_buyreq :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Buyrequest doesnt exists',
                code=400,
                ok=False
            )

        buyreq_name = 'BUY_' + already_user_obj['name']
        remove_buyRequest(buyreq_name)

        # TODO : When the graph has been manipulated, any staged graph analytic
        #        result must be rendered invalid, and new results must be
        #        calculated.
        set_graph_changed()

        return vedbjorn_pb2.GeneralInfoMessage(
            content='Buyrequest deleted',
            code=200,
            ok=True
        )

    def SellRequestToUser(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        already_user = get_user_with_email(request.contact_info.email_address)
        if not already_user:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='User doesnt exists',
                code=400,
                ok=False
            )
        already_user_obj = already_user[0][0]

        already_buyreq = get_buyrequests_with_email(request.contact_info.email_address)
        if already_buyreq :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='sellrequest can not be created, because a buyrequest already exist for this user',
                code=403,
                ok=False
            )

        already_sellreq = get_sellrequests_with_email(request.contact_info.email_address)
        sellreq_name = 'SELL_' + already_user_obj['name']
        if not already_sellreq:
            sellrequest_to_user(
                sellrequest = {
                    'name' : sellreq_name ,
                    'current_capacity' : request.current_capacity ,
                    'amount_reserved' : 0 ,
                    'amount_staged' : 0 ,
                    'num_reserved' : 0 ,
                    'num_staged' : 0 ,
                    'prepare_for_pickup' : 0 ,
                    'fake' : IS_FAKE
                } ,
                user = {
                    'email': already_user_obj['email'] ,
                    'phone' : already_user_obj['phone']
                } ,
                relationship_meta = {
                    'fake': IS_FAKE
                })

            # TODO : When the graph has been manipulated, any staged graph analytic
            #        result must be rendered invalid, and new results must be
            #        calculated.
            set_graph_changed()

            return vedbjorn_pb2.GeneralInfoMessage(
                content='sellrequest was created',
                code=200,
                ok=True
            )

        else:
            sellreq = already_sellreq[0][0]
            if request.current_capacity == sellreq['current_capacity'] :
                return vedbjorn_pb2.GeneralInfoMessage(
                    content='Capacity unchanged',
                    code=400,
                    ok=True
                )
            update_current_capacity_sellRequest(sellreq_name, request.current_capacity)

            # TODO : When the graph has been manipulated, any staged graph analytic
            #        result must be rendered invalid, and new results must be
            #        calculated.
            set_graph_changed()

            return vedbjorn_pb2.GeneralInfoMessage(
                content='current_capacity was updated',
                code=200,
                ok=True
            )

    def GetSellRequest(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.SellRequest(
                info = vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        already_user = get_user_with_email(request.email_address)
        if not already_user:
            return vedbjorn_pb2.SellRequest(
                contact_info=vedbjorn_pb2.UserContactInfo(email_address='', phone_number=''),
                name = '',
                current_capacity=0,
                amount_reserved=0,
                amount_staged=0,
                num_reserved=0,
                num_staged=0,
                prepare_for_pickup=0,
                fake=True,
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='User doesnt exist',
                    code=404,
                    ok=False
                )
            )

        already_user_obj = already_user[0][0]

        sellreq = get_sellrequests_with_email(request.email_address)
        if not sellreq:
            return vedbjorn_pb2.SellRequest(
                contact_info=vedbjorn_pb2.UserContactInfo(email_address='', phone_number=''),
                name = '',
                current_capacity=0,
                amount_reserved=0,
                amount_staged=0,
                num_reserved=0,
                num_staged=0,
                prepare_for_pickup=0,
                fake=True,
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='Sellrequest doesnt exist',
                    code=404,
                    ok=False
                )
            )
        sellreqObj = sellreq[0][0]
        return vedbjorn_pb2.SellRequest(
            contact_info=vedbjorn_pb2.UserContactInfo(
                email_address = already_user_obj['email'],
                phone_number = already_user_obj['phone']
            ),
            name = '',
            current_capacity=sellreqObj['current_capacity'],
            amount_reserved=sellreqObj['amount_reserved'],
            amount_staged=sellreqObj['amount_staged'],
            num_reserved=sellreqObj['num_reserved'],
            num_staged=sellreqObj['num_staged'],
            prepare_for_pickup=sellreqObj['prepare_for_pickup'],
            fake=is_fake(sellreqObj),
            info=vedbjorn_pb2.GeneralInfoMessage(
                content='Sellrequest found',
                code=200,
                ok=True
            )
        )

    def DeleteSellRequest(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        already_user = get_user_with_email(request.email_address)
        if not already_user:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='User doesnt exists',
                code=400,
                ok=False
            )
        already_sellreq = get_sellrequests_with_email(request.email_address)
        if not already_sellreq:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Sellrequest doesnt exists',
                code=400,
                ok=False
            )

        sellreq_name = 'SELL_' + already_user[0][0]['name']
        remove_sellRequest(sellreq_name)

        # TODO : When the graph has been manipulated, any staged graph analytic
        #        result must be rendered invalid, and new results must be
        #        calculated.
        set_graph_changed()

        return vedbjorn_pb2.GeneralInfoMessage(
            content='Sellrequest deleted',
            code=200,
            ok=True
        )

    def DriveRequestToUser(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        already_user = get_user_with_email(request.contact_info.email_address)
        if not already_user:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='User doesnt exists',
                code=400,
                ok=False
            )

        already_user_obj = already_user[0][0]

        already_drivereq = get_driverequests_with_email(request.contact_info.email_address)
        drivereq_name = 'DRIVE_' + already_user_obj['name']
        if not already_drivereq:
            driverequest_to_user(
                driverequest = {
                    'name' : drivereq_name ,
                    'available' : request.available ,
                    'num_staged_pickups' : 0 ,
                    'fake' : IS_FAKE
                } ,
                user = {
                    'email' : already_user_obj['email'] ,
                    'phone' : already_user_obj['phone']
                },
                relationship_meta = {
                    'fake': IS_FAKE
                })

            # TODO : When the graph has been manipulated, any staged graph analytic
            #        result must be rendered invalid, and new results must be
            #        calculated.
            set_graph_changed()

            return vedbjorn_pb2.GeneralInfoMessage(
                content='driverequest was created',
                code=200,
                ok=True
            )
        else:
            drivereq = already_drivereq[0][0]
            if request.available == drivereq['available'] :
                return vedbjorn_pb2.GeneralInfoMessage(
                    content='Driver availability unchanged',
                    code=400,
                    ok=True
                )
            update_available_driveRequest(drivereq_name, request.available)

            # TODO : When the graph has been manipulated, any staged graph analytic
            #        result must be rendered invalid, and new results must be
            #        calculated.
            set_graph_changed()

            return vedbjorn_pb2.GeneralInfoMessage(
                content='Driver availability was updated',
                code=200,
                ok=True
            )

    def GetDriveRequest(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.DriveRequest(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=40,
                    ok=False
                )
            )

        already_user = get_user_with_email(request.email_address)
        if not already_user:
            return vedbjorn_pb2.DriveRequest(
                contact_info=vedbjorn_pb2.UserContactInfo(email_address='', phone_number=''),
                name = '' ,
                available=False,
                num_staged_pickups=0,
                fake=True,
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='User doesnt exist',
                    code=404,
                    ok=False
                )
            )

        already_user_obj = already_user[0][0]

        drivereq = get_driverequests_with_email(request.email_address)
        if not drivereq:
            return vedbjorn_pb2.DriveRequest(
                contact_info=vedbjorn_pb2.UserContactInfo(email_address='', phone_number=''),
                name = '',
                available=False,
                num_staged_pickups=0,
                fake=True,
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='Driverequest doesnt exist',
                    code=404,
                    ok=False
                )
            )
        drivereqObj = drivereq[0][0]
        return vedbjorn_pb2.DriveRequest(
            contact_info=vedbjorn_pb2.UserContactInfo(
                email_address = already_user_obj['email'],
                phone_number = already_user_obj['phone']
            ),
            name = drivereqObj['name'] ,
            available=drivereqObj['available'],
            num_staged_pickups=drivereqObj['num_staged_pickups'],
            fake=is_fake(drivereqObj),
            info=vedbjorn_pb2.GeneralInfoMessage(
                content='Driverequest found',
                code=200,
                ok=True
            )
        )

    def DeleteDriveRequest(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=True
            )

        already_user = get_user_with_email(request.email_address)
        if not already_user:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='User doesnt exists',
                code=400,
                ok=False
            )
        already_drivereq = get_driverequests_with_email(request.email_address)
        if not already_drivereq:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Driverequest doesnt exists',
                code=400,
                ok=False
            )

        drivereq_name = 'DRIVE_' + already_user[0][0]['name']
        remove_driveRequest(drivereq_name)

        # TODO : When the graph has been manipulated, any staged graph analytic
        #        result must be rendered invalid, and new results must be
        #        calculated.
        set_graph_changed()

        return vedbjorn_pb2.GeneralInfoMessage(
            content='Driverequest deleted',
            code=200,
            ok=True
        )

    def SetDriverNotAvailable(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=True
            )

        if 'DRIVE_' in request.email_address and not '@' in request.email_address:

            drivereq_name = request.email_address
            set_driver_available(drivereq_name, False)
            loc = get_driver_location(drivereq_name)
            available_drivers = get_drivers_in_county(loc[0][0]['county'])
            if len(available_drivers) < 1:
                set_driver_available_again_time(request.email_address, datetime.datetime.utcnow().timestamp() + 10)
            else:
                set_driver_available_again_time(request.email_address, datetime.datetime.utcnow().timestamp() + DRIVER_QUARANTINE_TIME)

            # TODO : When the graph has been manipulated, any staged graph analytic
            #        result must be rendered invalid, and new results must be
            #        calculated.
            set_graph_changed()

            return vedbjorn_pb2.GeneralInfoMessage(
                content='Driverequest set to be temporarily unavailable',
                code=200,
                ok=True
            )

        already_user = get_user_with_email(request.email_address)
        if not already_user:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='User doesnt exists',
                code=400,
                ok=False
            )
        already_drivereq = get_driverequests_with_email(request.email_address)
        if not already_drivereq:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Driverequest doesnt exists',
                code=400,
                ok=False
            )

        drivereq_name = 'DRIVE_' + already_user[0][0]['name']
        set_driver_available(drivereq_name, False)
        set_driver_available_again_time(drivereq_name , datetime.datetime.utcnow().timestamp() + DRIVER_QUARANTINE_TIME)

        # TODO : When the graph has been manipulated, any staged graph analytic
        #        result must be rendered invalid, and new results must be
        #        calculated.
        set_graph_changed()

        return vedbjorn_pb2.GeneralInfoMessage(
            content='Driverequest set to be temporarily unavailable',
            code=200,
            ok=True
        )

    # TODO : NOTIFY ON EMAIL
    #
    #
    #
    #   rpc NotifyDriverOnNewMission(Name) returns (GeneralInfoMessage) {}
    def NotifyDriverOnNewMission(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=True
            )

        # TODO
        # TODO
        # TODO

        return vedbjorn_pb2.GeneralInfoMessage(
            content='Driver was notified',
            code=200,
            ok=True
        )

    def createRoute(self, obj : dict , driverName : str) -> vedbjorn_pb2.Routes:
        """

        :param obj:
        :param driverName:
        :return:
        """
        if not obj:
            return vedbjorn_pb2.Routes(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='Route not found',
                    code=403,
                    ok=False))
        calc_time = 0
        if 'calc_time' in obj and isinstance(obj['calc_time'] , datetime.datetime) :
            calc_time = obj['calc_time'].timestamp()
        elif 'calc_time' in obj :
            calc_time = float(obj['calc_time'])

        ret = vedbjorn_pb2.Routes(
            id=str(obj['_id']),
            driveRequestName=driverName,
            status = obj.get('status' , '') ,
            created_UTC=obj.get('created_UTC', 0),
            calc_time=calc_time,
            fake=is_fake(obj),
            updated=obj.get('updated', 0),
            accept_deadline=obj.get('accept_deadline', ),
            due=obj.get('due' , 0),
            wrapup=str(obj.get('wrapup' , '')) ,
            finished_time=obj.get('finished_time' , 0) ,
            finished_time_str=obj.get('finished_time_str' , '') ,
            info=vedbjorn_pb2.GeneralInfoMessage(
                content='Route found',
                code=200,
                ok=True))
        for rt in obj.get('route', []):
            from_loc: vedbjorn_pb2.Location = dict_to_Location(rt.get('from', {}))
            to_loc: vedbjorn_pb2.Location = dict_to_Location(rt.get('to', {}))
            visit = vedbjorn_pb2.Visit(
                from_loc=from_loc,
                to_loc=to_loc,
                distance=rt.get('distance', 0),
                type=rt['type'],
                loaded_before=rt['loaded_before'],
                loaded_after=rt['loaded_after'],
                sellRequest=dict_to_Sellrequest(rt['sellRequest']),
                driveRequest=dict_to_Driverequest(rt['driveRequest']),
                drive_user=dict_to_User(rt['drive_user']),
                buyRequest=dict_to_Buyrequest(rt.get('buyRequest', {})) ,
                visited=str(rt.get('visited' , '')) ,
                visited_status=rt.get('visited_status' , '') ,
                status=rt.get('status' , ''),
                return_amount=rt.get('return_amount' , 0) ,
                notification=str(rt.get('notification' , ''))
            )
            ret.route.append(visit)

        for sellName, deal in obj.get('deals', {}).items():
            sdeal = vedbjorn_pb2.SellerDeal(
                sellName=sellName,
                sellRequest=dict_to_Sellrequest(deal.get('sellRequest', {})),
                number_of_bags_sold=deal.get('number_of_bags_sold', 0)
            )
            for sell in deal.get('sells', []):
                onesell = vedbjorn_pb2.OneSell(
                    reserved_weeks=sell.get('reserved_weeks', -1),
                    last_calced=sell.get('last_calced', 0),
                    claimed_by_driver=sell.get('claimed_by_driver', False),
                    reserve_target=sell.get('reserve_target', ''),
                    current_requirement=sell.get('current_requirement', 0),
                    name=sell.get('name', ''),
                    fake=is_fake(sell)
                )
                sdeal.sells.append(onesell)
            ret.deals.append(sdeal)
        return ret

    def GetOngoingRoute(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.Routes(
                info = vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            ))

        def sort_by_created_UTC(e):
            return e.get('created_UTC', 0)

        email = request.value
        driveGraph = get_driverequests_with_email(email)
        if not driveGraph:
            return self.createRoute({}, '')
        driverObj = driveGraph[0][0]
        driverName = driverObj['name']

        db = get_db()
        candidates : list = []
        wrapups : list = []
        obj_it = db.insist_on_find('ongoing_routes', {'driveRequestName': driverName})
        for obj in mpcur(obj_it) :
            if 'wrapup' in obj :
                wrapups.append(obj)
            else:
                candidates.append(obj)
        candidates.sort(key=sort_by_created_UTC)
        wrapups.sort(key=sort_by_created_UTC)
        if len(candidates) == 0:
            if len(wrapups) == 0:
                return self.createRoute({}, driverName)
            else:
                return self.createRoute(wrapups[0], driverName)
        elif len(candidates) > 1 :
            print('WARNING : Driver ' , driverName, ' has more than 1 ongoing route. Should not be possible.')
        return self.createRoute(candidates[0], driverName)

    def GetCompletedRoutes(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        email = request.value
        driveGraph = get_driverequests_with_email(email)
        if not driveGraph:
            return self.createRoute({}, '')
        driverObj = driveGraph[0][0]
        driverName = driverObj['name']

        db = get_db()
        rlist = vedbjorn_pb2.RoutesList()
        obj_it = db.insist_on_find('ongoing_routes', {'driveRequestName': driverName})
        for obj in mpcur(obj_it) :
            if 'wrapup' in obj :
                rlist.routesList.append(self.createRoute(obj, driverName))
        return rlist

    def GetPlannedRoute(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        email = request.value
        driveGraph = get_driverequests_with_email(email)
        if not driveGraph :
            return self.createRoute({}, '')
        driverObj = driveGraph[0][0]
        driverName = driverObj['name']

        db = get_db()
        obj = db.insist_on_find_one_q('planned_routes' , {'driveRequestName' : driverName})
        if obj :
            num_sellers = obj.get('num_sellers', 0)
            num_sellers_accepted = obj.get('num_sellers_accepted', 0)
            if num_sellers <= 0 or num_sellers_accepted <= 0:
                return self.createRoute({}, driverName)
            if num_sellers_accepted < num_sellers:
                return self.createRoute({}, driverName)
        return self.createRoute(obj, driverName)

    def SetAcceptPlannedRoute(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        email = request.content
        driveGraph = get_driverequests_with_email(email)
        if not driveGraph:
            return self.createRoute({}, '')
        driverObj = driveGraph[0][0]
        driverName = driverObj['name']

        accepted = request.code == 1
        db = get_db()
        obj = db.insist_on_find_one_q('planned_routes', {'driveRequestName': driverName})
        if not obj:
            return vedbjorn_pb2.GeneralInfoMessage(
                    content='Route not found',
                    code=403,
                    ok=False)
        if accepted:
            claim_planned_route(obj, IS_FAKE)
        else:
            decline_planned_route(obj)
        return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=200,
                ok=True)

    # rpc PushVisit(VisitProof) returns (GeneralInfoMessage) {}
    def PushVisit(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        email = request.driverName
        driveGraph = get_driverequests_with_email(email)
        if not driveGraph:
            return self.createRoute({}, '')
        driverObj = driveGraph[0][0]
        driverName = driverObj['name']

        img = request.img
        visitIndex = request.visitIndex
        #driverName = request.driverName
        type = request.type
        img_text = request.img_text
        timestamp = request.timestamp

        db = get_db()
        ongoing_route = db.insist_on_find_one_q('ongoing_routes' , {
            'driveRequestName' : driverName ,
            'wrapup' : {'$exists' : False}
        })
        if not ongoing_route :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Ongoing route not found',
                code=400,
                ok=True)

        if not 'route' in ongoing_route or visitIndex < 0 or visitIndex >= len(ongoing_route['route']) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Visit in ongoing route not found',
                code=400,
                ok=True)

        visit = ongoing_route['route'][visitIndex]
        img_meta : dict = {
            'ongoing_route' : ongoing_route['_id'] ,
            'index' : visitIndex ,
            'timestamp' : timestamp ,
            'driverName' : driverName ,
            'type' : type ,
            'img_text' : img_text
        }
        if img:
            file_id = db.insist_on_insert_file(img, 'proofimage.jpeg' , 'jpeg', img_meta)
            img_meta['file'] = file_id

        handle_ret = None
        if type == 'pickup' :
            handle_ret = handle_pickup(visit, driverName, visitIndex, ongoing_route, IS_FAKE ,
                          datetime.datetime.utcfromtimestamp(timestamp), img_meta)

        elif type == 'delivery' :
            handle_ret = handle_delivery(visit, driverName, visitIndex, ongoing_route, IS_FAKE ,
                          datetime.datetime.utcfromtimestamp(timestamp), img_meta)

        elif type == 'return' :
            handle_ret = handle_return(visit, driverName, visitIndex, ongoing_route, IS_FAKE,
                            datetime.datetime.utcfromtimestamp(timestamp), img_meta)

        else:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Did not recognize visit type',
                code=400,
                ok=False)

        return vedbjorn_pb2.GeneralInfoMessage(
            content='',
            code=200,
            ok=True)

    """
    rpc GetDeliveryProof(Name) returns (VisitProof) {}
    """
    def GetDeliveryProof(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.VisitProof(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False))

        delivery_id = request.value
        if not delivery_id :
            return vedbjorn_pb2.VisitProof(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='Empty deliveries.id',
                    code=400,
                    ok=False))
        db = get_db()
        visitObj = db.insist_on_find_one('deliveries', delivery_id)
        if not visitObj :
            return vedbjorn_pb2.VisitProof(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='Delivery not found',
                    code=400,
                    ok=False))
        if 'payment_ref' in visitObj :
            payment = db.insist_on_find_one('vipps_payments_in' , visitObj['payment_ref'])
            if payment.get('status' , '') == 'paid' :
                """
                When the delivery has been paid, it means the client has already accepted the delivery and therefore
                also seen and acknowledged the proof. It is now to be considered an historic event
                """
                return vedbjorn_pb2.VisitProof(
                    info=vedbjorn_pb2.GeneralInfoMessage(
                        content='Already paid',
                        code=400,
                        ok=True))

        if 'file' in visitObj :
            content = db.insist_on_get_filecontent_id(visitObj['file'])
        elif 'file' in visitObj.get('meta' , {}) :
            content = db.insist_on_get_filecontent_id(visitObj['meta']['file'])
        else:
            content = b''

        return vedbjorn_pb2.VisitProof(
            img = content ,
            img_text = visitObj['meta']['img_text'] ,
            timestamp = visitObj['meta']['timestamp'] ,
            info=vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=200,
                ok=True))

    #   rpc GetVisit(VisitIndex) returns (VisitProof) {}
    def GetVisit(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.VisitProof(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False))

        email = request.driverName
        driveGraph = get_driverequests_with_email(email)
        if not driveGraph:
            return self.createRoute({}, '')
        driverObj = driveGraph[0][0]
        driverName = driverObj['name']

        visitIndex = request.index
        #driverName = request.driverName

        db = get_db()
        ongoing_route = db.insist_on_find_one_q('ongoing_routes', {
            'driveRequestName': driverName,
            'wrapup': {'$exists': False} # Because its not a wrapped-up route we want to check the visit for. A wrapped up route can not be "revisited".
        })
        if not ongoing_route:
            return vedbjorn_pb2.VisitProof(
                info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Ongoing route not found',
                    code=400,
                    ok=True))

        if not 'route' in ongoing_route or visitIndex < 0 or visitIndex >= len(ongoing_route['route']):
            return vedbjorn_pb2.VisitProof(
                info = vedbjorn_pb2.GeneralInfoMessage(
                content='Visit in ongoing route not found',
                code=400,
                ok=True))

        visit = ongoing_route['route'][visitIndex]
        if not visit or not 'visited_status' in visit or not 'visited' in visit:
            return vedbjorn_pb2.VisitProof(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='Visit has not yet taken place',
                    code=400,
                    ok=True))

        if visit['type'] == 'pickup' :
            visitObj = db.insist_on_find_one('pickups' , visit['visited'])
        elif visit['type'] == 'delivery':
            visitObj = db.insist_on_find_one('deliveries' , visit['visited'])
        else:
            return vedbjorn_pb2.VisitProof(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='Visit has not yet taken place',
                    code=400,
                    ok=False
                )
            )

        if not visitObj :
            return vedbjorn_pb2.VisitProof(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='Could not locate the visit in the database',
                    code=500,
                    ok=True))

        content = db.insist_on_get_filecontent_id(visitObj['meta']['file'])

        return vedbjorn_pb2.VisitProof(
            img = content ,
            visitIndex = visitIndex ,
            driverName = driverName ,
            type = visit['type'] ,
            img_text = visitObj['meta']['img_text'] ,
            timestamp = visitObj['meta']['timestamp'] ,
            info=vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=200,
                ok=True))

    """
    rpc PushFeedbackComplaintNondelivery(FeedbackComplaintNondelivery) returns (GeneralInfoMessage) {}
    message FeedbackComplaintNondelivery {
          string buyerEmail = 1;
          string ongoingRouteIt = 2;
          GeneralInfoMessage info = 3;
        };
    """
    def PushFeedbackComplaintNondelivery(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        buyerEmail = request.buyerEmail
        ongoingRouteIt = request.ongoingRouteIt
        db = get_db()
        if not db.is_ObjectId(ongoingRouteIt) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Assignment not found',
                code=400,
                ok=False)
        already_user = get_user_with_email(buyerEmail)
        if not already_user:
            return vedbjorn_pb2.GeneralInfoMessage(
                content='User doesnt exists',
                code=400,
                ok=False)

        already_buyreq = get_buyrequests_with_email(buyerEmail)
        if not already_buyreq :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Buyrequest not found',
                code=400,
                ok=False)

        userObj = already_user[0][0]
        buyreqObj = already_buyreq[0][0]

        ongoing_route = db.insist_on_find_one('ongoing_routes' , ongoingRouteIt)
        if not ongoing_route :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Assignment not found',
                code=400,
                ok=False)

        driveReqName = ongoing_route['driveRequestName']
        driver_user = get_user_with_driverequest_name(driveReqName)

        _nowts = datetime.datetime.utcnow().timestamp()
        due = ongoing_route.get('due' , _nowts)
        if due >= _nowts :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Rejected : Due not reached',
                code=400,
                ok=True)

        buyer_notification = db.insist_on_find_one_q('notifications' , {
            'email' : buyerEmail ,
            'status' : 'requested'
        })
        if buyer_notification :
            # this user should no longer be notified that there is a match
            db.insist_on_delete_one('notifications' , buyer_notification['_id'])

        index : int = -1
        driverUser = None
        for visit in ongoing_route['route'] :
            index = index + 1
            if visit.get('buyRequest' , {}).get('name' , '') == buyreqObj['name'] :
                db.insist_on_set_attribute_in_array_at_index(
                    id             = ongoing_route['_id'] ,
                    col            = 'ongoing_routes'     ,
                    index          = index                ,
                    arrayName      = 'route'              ,
                    attributeName  = 'status'             ,
                    attributeValue = 'withdrawn'          )

                if driver_user :
                    driverUser = driver_user[0][0]
                    notification_id = db.insist_on_insert_one('notifications' , {
                        'email' : driverUser['email'] ,
                        'timestamp': datetime.datetime.utcnow().timestamp(),
                        'ongoing_routes' : ongoing_route['_id'] ,
                        'status' : 'cancelled' ,
                        'contentType': 'driver',
                        'text' : 'Kunden har trukket kjøpet på grunn av for sen levering.' ,
                        'route_index' : index ,
                        'canceller' : userObj.get('email' , buyerEmail)
                    })
                    db.insist_on_set_attribute_in_array_at_index(
                        id             = ongoing_route['_id'] ,
                        col            = 'ongoing_routes'     ,
                        index          = index                ,
                        arrayName      = 'route'              ,
                        attributeName  = 'notification'       ,
                        attributeValue = notification_id      )
                break

        """
        Since this customer no longer will get a delivery, we must figure out :
        1. If the driver has already picked up the firewood which has now been cancelled. If so :
                Add a visit in the end of the route to return the correct amount.
        2. If the driver has not yet picked up the firewood which has now been cancelled. If so :
                Reduce the amount to be picked up at this seller location accordingly
        """
        sellReq = None
        stahp = False
        for sell_name, sellInfo in ongoing_route.get('deals' , {}).items() :
            if stahp :
                break
            for sell in sellInfo.get('sells' , []) :
                if sell.get('name' , '_') == buyreqObj['name'] :
                    sellReq = sellInfo.get('sellRequest' , None)
                    stahp = True
                    break
        new_visit = None
        delete_index = -1
        if sellReq :
            index: int = -1
            for visit in ongoing_route['route']:
                index = index + 1
                if visit.get('sellRequest', {}).get('name', '') == sellReq['name']:
                    if visit.get('visited_status') == 'completed' :
                        new_visit = visit
                        if 'visited' in new_visit :
                            del new_visit['visited']
                        if 'visited_status' in new_visit :
                            del new_visit['visited_status']
                        new_visit['type'] = 'return'
                        new_visit['return_amount'] = buyreqObj['current_requirement']
                    else:
                        loaded_after = visit['loaded_after']
                        new_loaded_after = loaded_after - buyreqObj['current_requirement']
                        if new_loaded_after <= 0:
                            delete_index = index
                        else:
                            db.insist_on_set_attribute_in_array_at_index(
                                id             = ongoing_route['_id'] ,
                                col            = 'ongoing_routes'     ,
                                index          = index                ,
                                arrayName      = 'route'              ,
                                attributeName  = 'loaded_after'       ,
                                attributeValue = new_loaded_after     )
                    break
            if delete_index >= 0 or new_visit != None :
                ongoing_route = db.insist_on_find_one('ongoing_routes', ongoingRouteIt)
                updated_route = ongoing_route['route']
                if delete_index >= 0 :
                    del updated_route[delete_index]
                    db.insist_on_update_one(ongoing_route, 'ongoing_routes', 'route' , updated_route)
                elif new_visit :
                    index: int = -1
                    for visit in ongoing_route['route']:
                        index = index + 1
                        if visit.get('type' , '_') == 'return' and visit.get('sellRequest' , {}).get('name' , '_') == \
                                                               new_visit.get('sellRequest' , {}).get('name' , 'X') :
                            new_return_amount = visit['return_amount'] + new_visit['return_amount']
                            db.insist_on_set_attribute_in_array_at_index(
                                id             = ongoing_route['_id'] ,
                                col            = 'ongoing_routes'     ,
                                index          = index                ,
                                arrayName      = 'route'              ,
                                attributeName  = 'return_amount'      ,
                                attributeValue = new_return_amount    )
                            return vedbjorn_pb2.GeneralInfoMessage(
                                content='',
                                code=200,
                                ok=True)
                    new_ongoing_route = ongoing_route['route']
                    new_ongoing_route.append(new_visit)
                    db.insist_on_update_one(ongoing_route, 'ongoing_routes' , 'route' , new_ongoing_route)

        num_cancelled : int = 0
        num_tot : int = 0
        num_picked_up : int = 0
        ongoing_route = db.insist_on_find_one('ongoing_routes', ongoingRouteIt)
        for visit in ongoing_route['route'] :
            index = index + 1
            if visit.get('type' , '_') == 'delivery' :
                if visit.get('status' , '_') == 'withdrawn' :
                    num_cancelled = num_cancelled + 1
                num_tot = num_tot + 1
            if visit.get('type' , '_') == 'pickup' and visit.get('visited_status') == 'completed':
                num_picked_up = num_picked_up + 1

        if num_cancelled == num_tot :
            """
            All buyers has withdrawn. Then we must cancel this ongoing route, and put the driver in quarantine
            """
            notification_id = db.insist_on_insert_one('notifications', {
                'email': driverUser['email'],
                'timestamp' : datetime.datetime.utcnow().timestamp() ,
                'ongoing_routes': ongoing_route['_id'],
                'status': 'cancelled',
                'contentType' : 'driver' ,
                'text': 'Oppdraget ditt slettes fordi all kundene har trukket kjøpet. Vennligst lever tilbake all veden du har plukket opp snarest',
                'canceller': userObj.get('email', buyerEmail)
            })
            db.insist_on_update_one(ongoing_route, 'ongoing_routes', 'status', 'failed')
            db.insist_on_update_one(ongoing_route, 'ongoing_routes', 'status_notification', notification_id)
            if num_picked_up <= 0 :
                set_driver_available(driveReqName, False)
                set_driver_available_again_time(driveReqName, datetime.datetime.utcnow().timestamp() + DRIVER_QUARANTINE_TIME)
            else:
                """
                Thieving driver! 
                """
                set_driver_available(driveReqName, False)
                set_driver_available_again_time(driveReqName, datetime.datetime(year=2100, month=1, day=1).timestamp())

        """
        Check ongoing_route is finished
        """
        is_done, msg = verify_that_route_is_completed(ongoing_route)

        if is_done:

            #
            # We pretend that all deliveries has been paid by the receiving clients
            #
            CHEATING__set_all_delivery_payments_to_completed(ongoing_route)
            #
            #
            #

            wrap_up_ongoing_route(str(ongoing_route['_id']), IS_FAKE)
            db.insist_on_update_one(ongoing_route, 'ongoing_routes', 'status', 'finished')
            plan = db.insist_on_find_one_q('planned_routes', {'driveRequestName': ongoing_route['driveRequestName']})
            if plan:
                db.insist_on_delete_one('planned_routes', plan['_id'])

        return vedbjorn_pb2.GeneralInfoMessage(
            content='',
            code=200,
            ok=True)

    """
    rpc GetMessages(MessageQuery)  returns (Messages) {}
    message Message {
      double timestamp = 1;
      string email = 2;
      string emailSender = 3;
      string contentType = 4;
      double amount = 5;
      string ref_collection = 6;
      string ref_id = 7;
      string text = 8;
      GeneralInfoMessage info = 9;
    };

    message MessageQuery {
      string receiverEmail = 1;
      string senderEmail = 2;
      double from_time = 3;
      double to_time = 4;
      repeated uint32 indices = 5;
      string action = 6;
      GeneralInfoMessage info = 7;
    };

    message Messages {
      repeated Message messages = 1;
    };
    """
    def GetMessages(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.Messages(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        receiverEmail = request.receiverEmail
        senderEmail = request.senderEmail
        from_time = request.from_time
        to_time = request.to_time
        action = request.action

        query : dict = {}
        if receiverEmail :
            query['email'] = receiverEmail

        # TODO : Make query based on the other optional variables

        messages = vedbjorn_pb2.Messages()
        db = get_db()
        notif_it = db.insist_on_find('notifications' , query)
        for notif in mpcur(notif_it) :
            refcol : str = ''
            refid : str = ''
            if 'ongoing_routes' in refcol :
                refcol = 'ongoing_routes'
                refid = notif['ongoing_routes']
            messages.messages.append(vedbjorn_pb2.Message(
                timestamp = notif.get('timestamp' , 0) ,
                email = notif.get('email' , '') ,
                emailSender = notif.get('canceller' , notif.get('sender' , '')) ,
                contentType = notif.get('contentType' , '') ,
                amount = notif.get('amount', notif.get('route_index' , 0)) ,
                ref_collection = refcol ,
                ref_id = refid ,
                text = notif.get('text' , '') ,
                status = notif.get('status' , '')
            ))
        return messages

    """
    rpc GetBuyRequestNotification(MessageQuery) returns (Message) {}
    """
    def GetBuyRequestNotification(self, request, context):
        """

        NOTE : When the buying user is asking for a buyrequest-notification it is implicitly always asking for those
        notifications associated with a buy which has not yet been completed.

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.Message(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        receiverEmail = request.receiverEmail
        senderEmail = request.senderEmail
        from_time = request.from_time
        to_time = request.to_time
        action = request.action

        query: dict = {}
        if receiverEmail:
            query['email'] = receiverEmail

        # TODO : Make query based on the other optional variables

        db = get_db()
        notif_it = db.insist_on_find('notifications' , {
            'email' : receiverEmail ,
            'contentType' : 'delivery' ,
            'ref_collection' : 'deliveries'
        })
        for notif in mpcur(notif_it) :
            delivery = db.insist_on_find_one('deliveries' , notif.get('ref_id' , None))
            if not delivery :
                continue

            #
            # To know that we have the notification which must be processed, we can check the payment-status
            # If it is not paid yet, then its the correct one!
            #
            payment = db.insist_on_find_one('vipps_payments_in', delivery.get('payment_ref', None))
            if payment and payment.get('status' , '') == 'unpaid' :
                return vedbjorn_pb2.Message(
                        timestamp = notif.get('timestamp' , 0) ,
                        email = notif.get('email' , '') ,
                        emailSender = notif.get('canceller' , notif.get('sender' , '')) ,
                        contentType = notif.get('contentType' , '') ,
                        amount = notif.get('amount', notif.get('route_index' , 0)) ,
                        ref_collection = 'deliveries' ,
                        ref_id = str(notif.get('ref_id' , '')) ,
                        text = notif.get('text' , '') ,
                        status = notif.get('status' , ''))

        return vedbjorn_pb2.Message(
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = 'no delivery' ,
                code = 404 ,
                ok = False
            )
        )

    """
        rpc PushFeedbackAcceptDelivery(MessageQuery) returns (GeneralInfoMessage) {}
        
        message MessageQuery {
          string receiverEmail = 1;
          string senderEmail = 2;
          double from_time = 3;
          double to_time = 4;
          repeated uint32 indices = 5;
          string action = 6;
          GeneralInfoMessage info = 7;
        };
    """
    def PushFeedbackAcceptDelivery(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        receiverEmail = request.receiverEmail
        senderEmail = request.senderEmail
        from_time = request.from_time
        to_time = request.to_time
        action = request.action

        buyer_location = get_user_location(receiverEmail)
        buyer_address = ''
        if buyer_location:
            buyer_address = buyer_location[0][0]['display_name']

        db = get_db()
        notification = db.insist_on_find_one('notifications', action)
        if not notification:

            delivery = db.insist_on_find_one('deliveries', action)
            if not delivery:
                return vedbjorn_pb2.GeneralInfoMessage(
                    content="Delivery not found",
                    code=403,
                    ok=False
                )

            notification = db.insist_on_find_one('notifications', delivery['notification'])
            if not notification:
                return vedbjorn_pb2.GeneralInfoMessage(
                    content="Notification not found",
                    code=403,
                    ok=False
                )

        else :
            delivery = db.insist_on_find_one('deliveries' , notification.get('ref_id' , ''))
            if not delivery :
                return vedbjorn_pb2.GeneralInfoMessage(
                    content="Delivery not found",
                    code=403,
                    ok=False
                )

        payment = {}
        if 'payment_ref' in delivery :
            payment = db.insist_on_find_one('vipps_payments_in' , delivery['payment_ref'])

        ongoing_route = db.insist_on_find_one('ongoing_routes' , delivery['ongoing_route'])
        if not ongoing_route :
            return vedbjorn_pb2.GeneralInfoMessage(
                content="Could not locate the assignment where this delivery took place",
                code=403,
                ok=False
            )

        """
        Update relevant documents with the rejection-details
        """
        accept_ref = db.insist_on_insert_one('delivery_accept', {
            'buyer': receiverEmail,
            'notification': notification['_id'],
            'payment': payment['_id'],
            'ongoing_route': ongoing_route['_id'],
            'timestamp': datetime.datetime.utcnow().timestamp()
        })
        db.insist_on_update_one(notification, 'notifications', 'status', 'accepted')
        db.insist_on_update_one(notification, 'notifications', 'accept', accept_ref)
        db.insist_on_update_one(delivery, 'deliveries', 'accept', accept_ref)
        if payment:
            db.insist_on_update_one(payment, 'vipps_payments_in', 'accept', accept_ref)
        index: int = -1
        seller_email: str = ''
        num_bags: int = 0
        for travel in ongoing_route.get('route', []):
            index = index + 1
            if str(travel.get('visited', '')) == str(delivery['_id']):
                db.insist_on_set_attribute_in_array_at_index(
                    id=ongoing_route['_id'],
                    col='ongoing_routes',
                    index=index,
                    arrayName='route',
                    attributeName='accepted',
                    attributeValue=accept_ref
                )
                sellReq_name = travel.get('sellRequest', {}).get('name', '')
                num_bags = travel.get('loaded_before', 0) - travel.get('loaded_after', 0)
                sellUser = get_user_with_sellrequest_name(sellReq_name)
                if sellUser:
                    seller_email = sellUser[0][0]['email']
                break

        driver = get_user_with_driverequest_name(ongoing_route['driveRequestName'])
        db.insist_on_insert_one('notifications', {
            'email': seller_email,
            'timestamp': datetime.datetime.utcnow().timestamp(),
            'contentType': 'accepted',
            'ref_collection': 'ongoing_routes',
            'ref_id': ongoing_route['_id'],
            'amount': index,
            'status': 'new',
            'meta': delivery.get('meta', {}),
            'text': 'En kunde (' + receiverEmail + ') har godkjent en leveranse med ved fra deg.'
                    + str(num_bags) + ' sekker ble levert av ' + driver[0][0]['email'] + '.' +
                    'Det betyr at veden din holder den kvaliteten den skal. Godt jobbet!'
        })

        ongoing_route = db.insist_on_find_one('ongoing_routes', delivery['ongoing_route'])
        is_done, msg = verify_that_route_is_completed(ongoing_route)
        if is_done:

            #
            # We pretend that all deliveries has been paid by the receiving clients
            #
            # TODO : The check for completeness must be performed by the payment service. The final payment of the
            # TODO : ..assignment determines the end of the ongoing_route.
            CHEATING__set_all_delivery_payments_to_completed(ongoing_route)
            #
            #
            #

            wrap_up_ongoing_route(str(ongoing_route['_id']), IS_FAKE)
            db.insist_on_update_one(ongoing_route, 'ongoing_routes', 'status', 'finished')
            plan = db.insist_on_find_one_q('planned_routes', {'driveRequestName': ongoing_route['driveRequestName']})
            if plan:
                db.insist_on_delete_one('planned_routes', plan['_id'])

        return vedbjorn_pb2.GeneralInfoMessage(
            content = "" ,
            code = 200,
            ok = True
        )

    """"
        rpc PushFeedbackRejectDelivery(FeedbackRejectDelivery) returns (GeneralInfoMessage) {}
        
        message FeedbackRejectDelivery {
          string buyerEmail = 1;
          string notif_id = 2;
          bool wrongAmount = 3;
          bool wrongPrice = 4;
          bool wrongQuality = 5;
          string customMessage = 6;
          GeneralInfoMessage info = 7;
        };
    """
    def PushFeedbackRejectDelivery(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        buyerEmail = request.buyerEmail
        wrongAmount = request.wrongAmount
        wrongPrice = request.wrongPrice
        wrongQuality = request.wrongQuality
        customMessage = request.customMessage

        buyer_location = get_user_location(buyerEmail)
        buyer_address = ''
        if buyer_location :
            buyer_address = buyer_location[0][0]['display_name']

        db = get_db()
        notification = db.insist_on_find_one('notifications' , request.notif_id)
        if not notification :
            return vedbjorn_pb2.GeneralInfoMessage(
                content="Notification not found",
                code=403,
                ok=False
            )

        if 'rejection' in notification :
            return vedbjorn_pb2.GeneralInfoMessage(
                content="Already rejected",
                code=403,
                ok=False
            )

        delivery = db.insist_on_find_one('deliveries' , notification.get('ref_id' , ''))
        if not delivery :
            return vedbjorn_pb2.GeneralInfoMessage(
                content="Delivery not found",
                code=403,
                ok=False
            )

        payment = {}
        if 'payment_ref' in delivery :
            payment = db.insist_on_find_one('vipps_payments_in' , delivery['payment_ref'])
            if payment.get('status' , '') == 'paid' :
                return vedbjorn_pb2.GeneralInfoMessage(
                    content="Already paid for",
                    code=403,
                    ok=False
                )

        ongoing_route = db.insist_on_find_one('ongoing_routes' , delivery['ongoing_route'])
        if not ongoing_route :
            return vedbjorn_pb2.GeneralInfoMessage(
                content="Could not locate the assignment where this delivery took place",
                code=403,
                ok=False
            )

        """
        Update relevant documents with the rejection-details
        """
        rejection_ref = db.insist_on_insert_one('delivery_rejections' , {
            'buyer' : buyerEmail ,
            'notification' : notification['_id'] ,
            'payment' : payment['_id'] ,
            'ongoing_route' : ongoing_route['_id'] ,
            'timestamp' : datetime.datetime.utcnow().timestamp() ,
            'wrongAmount' : wrongAmount ,
            'wrongPrice' : wrongPrice ,
            'wrongQuality' : wrongQuality,
            'customMessage' : customMessage
        })
        db.insist_on_update_one(notification, 'notifications', 'status', 'rejected')
        db.insist_on_update_one(notification, 'notifications' , 'rejection' , rejection_ref)
        db.insist_on_update_one(delivery, 'deliveries', 'rejection', rejection_ref)
        if payment :
            db.insist_on_update_one(payment, 'vipps_payments_in', 'rejection', rejection_ref)
            db.insist_on_update_one(payment, 'vipps_payments_in', 'status', 'rejected')
        index : int = -1
        seller_email : str = ''
        num_bags : int = 0
        for travel in ongoing_route.get('route' , []) :
            index = index + 1
            if str(travel.get('visited' , '')) == str(delivery['_id']) :
                db.insist_on_set_attribute_in_array_at_index(
                    id = ongoing_route['_id'] ,
                    col = 'ongoing_routes' ,
                    index = index ,
                    arrayName = 'route' ,
                    attributeName = 'rejection' ,
                    attributeValue = rejection_ref
                )
                sellReq_name = travel.get('sellRequest' , {}).get('name' , '')
                num_bags = travel.get('loaded_before' , 0) - travel.get('loaded_after' , 0)
                sellUser = get_user_with_sellrequest_name(sellReq_name)
                if sellUser :
                    seller_email = sellUser[0][0]['email']
                break

        """
        Send appropriate notifications
        """
        if wrongQuality == True :
            db.insist_on_insert_one('notifications' , {
                'email' : seller_email ,
                'timestamp' : datetime.datetime.utcnow().timestamp() ,
                'contentType' : 'rejection' ,
                'ref_collection' : 'ongoing_routes' ,
                'ref_id' : ongoing_route['_id'] ,
                'amount' : index ,
                'status' : 'new' ,
                'meta': delivery.get('meta', {}),
                'text' : 'En kunde (' + buyerEmail + ') har avslått en leveranse med ved fra deg fordi '
                         'kunden mener leveransen hadde en for lav kvalitet i forhold til det som kan aksepteres. '
                         'Vi minner om de produkt-krav som stilles, les mer om dette på hjemmesiden. I henhold til '
                         'våre retningslinjer for salg er du selv ansvarlig for å hente tilbake den veden som ikke '
                         'er godkjent levert. ' + str(num_bags) + ' sekker kan hentes på følgende adresse : ' + buyer_address
            })
        if wrongAmount == True :
            driver = get_user_with_driverequest_name(ongoing_route['driveRequestName'])
            db.insist_on_insert_one('notifications', {
                'email': driver[0][0]['email'],
                'timestamp': datetime.datetime.utcnow().timestamp(),
                'contentType': 'rejection',
                'ref_collection': 'ongoing_routes',
                'ref_id': ongoing_route['_id'],
                'amount': index,
                'status': 'new',
                'meta' : delivery.get('meta' , {}) ,
                'text': 'En kunde (' + buyerEmail + ') har avslått en leveranse med ved levert av deg fordi '
                        'kunden mener leveransen hadde uriktig antall vedsekker. Det skulle være ' + str(num_bags) + ' sekker.'
                        ' Det du kan gjøre for å fikse dette er å returnere til adressen ' + buyer_address + ' med riktig antall.'
                        ' Er du uenig i dette? Da kan du sende en epost til uenig@vedbjorn.no, merk eposten med følgende : ' +
                        str(ongoing_route['_id']) + '(' + str(index) + ')'
            })
            db.insist_on_set_attribute_in_array_at_index(
                id=ongoing_route['_id'],
                col='ongoing_routes',
                index=index,
                arrayName='route',
                attributeName='visited_name',
                attributeValue='wrong amount'
            )

        return vedbjorn_pb2.GeneralInfoMessage(
            content="",
            code=200,
            ok=True
        )

    """
        rpc GetAllCompletedDeliveryInfoForBuyer(MessageQuery) returns (AllCompletedDeliveryInfoForBuyer) {}
        
        message CompletedDeliveryInfoForBuyer {
          string email = 1;
          double time = 2;
          uint32 amount = 3;
          string driverEmail = 4;
          string sellerEmail = 5;
          string status = 6;
          double paidAmount = 7;
          string notifications_id = 8;
          string deliveries_id = 9;
          GeneralInfoMessage info = 10;
        };
        
        message AllCompletedDeliveryInfoForBuyer {
          repeated CompletedDeliveryInfoForBuyer deliveries = 1;
          GeneralInfoMessage info = 2;
        };
    """
    def GetAllCompletedDeliveryInfoForBuyer(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        ret = vedbjorn_pb2.AllCompletedDeliveryInfoForBuyer()
        if not self.let_the_client_in(context) :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )
            return ret

        receiverEmail = request.receiverEmail
        senderEmail = request.senderEmail
        from_time = request.from_time
        to_time = request.to_time
        action = request.action
        buyreq_name : str = ''
        buyreq = get_buyrequests_with_email(receiverEmail)
        if buyreq :
            buyreq_name = buyreq[0][0]['name']

        buffer_routes : dict = {}
        db = get_db()
        accepted_deliveres_it = db.insist_on_find('delivery_accept', {'buyer' : receiverEmail})
        for delivery in mpcur(accepted_deliveres_it) :
            driverEmail : str = ''
            sellerEmail : str = ''
            amount : int = 0
            paidAmount : float = 0
            deliveries_id = str(delivery['_id'])
            notifications_id : str = ''
            time = delivery.get('timestamp' , 0)
            if 'notification' in delivery :
                notifications_id = str(delivery['notification'])
            if 'payment' in delivery :
                payment = db.insist_on_find_one('vipps_payments_in' , delivery['payment'])
                if payment and payment.get('status' , '') == 'paid' and 'amount_NOK' in payment :
                    paidAmount = float(payment['amount_NOK'])
            if 'ongoing_route' in delivery :
                orid = delivery['ongoing_route']
                if not str(orid) in buffer_routes :
                    ongoing_route = db.insist_on_find_one('ongoing_routes', orid)
                    if ongoing_route :
                        buffer_routes[str(orid)] = ongoing_route
                if str(orid) in buffer_routes:
                    ongoing_route = buffer_routes[str(orid)]
                    driver = get_user_with_driverequest_name(ongoing_route['driveRequestName'])
                    if driver :
                        driverEmail = driver[0][0]['email']
                    for travel in ongoing_route['route'] :
                        if travel.get('buyRequest' , {}).get('name' , '_') == buyreq_name :
                            sellRequest_name = travel.get('sellRequest' , {}).get('name' , '_')
                            seller = get_user_with_sellrequest_name(sellRequest_name)
                            if seller :
                                sellerEmail = seller[0][0]['email']
                            amount = travel.get('loaded_before' , 0) - travel.get('loaded_after' , 0)
                            break
                else:
                    continue
            ret.deliveries.append(vedbjorn_pb2.CompletedDeliveryInfoForBuyer(
                email=receiverEmail,
                time=time,
                amount=amount,
                driverEmail=driverEmail,
                sellerEmail=sellerEmail,
                status='accepted',
                paidAmount=paidAmount,
                notifications_id=notifications_id,
                deliveries_id=deliveries_id
            ))

        rejected_deliveres_it = db.insist_on_find('delivery_rejections', {'buyer': receiverEmail})
        for delivery in mpcur(rejected_deliveres_it) :
            driverEmail : str = ''
            sellerEmail : str = ''
            amount : int = 0
            paidAmount : float = 0
            deliveries_id = str(delivery['_id'])
            notifications_id : str = ''
            time = delivery.get('timestamp' , 0)
            if 'notification' in delivery :
                notifications_id = str(delivery['notification'])
            if 'payment' in delivery :
                payment = db.insist_on_find_one('vipps_payments_in' , delivery['payment'])
                if payment and payment.get('status' , '') == 'paid' and 'amount_NOK' in payment :
                    paidAmount = float(payment['amount_NOK'])
            if 'ongoing_route' in delivery :
                orid = delivery['ongoing_route']
                if not str(orid) in buffer_routes :
                    ongoing_route = db.insist_on_find_one('ongoing_routes', orid)
                    if ongoing_route :
                        buffer_routes[str(orid)] = ongoing_route
                if str(orid) in buffer_routes:
                    ongoing_route = buffer_routes[str(orid)]
                    driver = get_user_with_driverequest_name(ongoing_route['driveRequestName'])
                    if driver :
                        driverEmail = driver[0][0]['email']
                    for travel in ongoing_route['route'] :
                        if travel.get('buyRequest' , {}).get('name' , '_') == buyreq_name :
                            sellRequest_name = travel.get('sellRequest' , {}).get('name' , '_')
                            seller = get_user_with_sellrequest_name(sellRequest_name)
                            if seller :
                                sellerEmail = seller[0][0]['email']
                            amount = travel.get('loaded_before' , 0) - travel.get('loaded_after' , 0)
                            break
                else:
                    continue

            msg : str = 'Decline-reason(s) :\n'
            if delivery.get('wrongAmount', False) == True :
                msg = msg + '* : Wrong Amount\n'
            if delivery.get('wrongPrice', False) == True :
                msg = msg + '* : Wrong Price\n'
            if delivery.get('wrongQuality', False) == True :
                msg = msg + '* : Wrong Quality\n'
            if delivery.get('customMessage' , '') != '' :
                msg = msg + 'Message : ' + delivery['customMessage']

            ret.deliveries.append(vedbjorn_pb2.CompletedDeliveryInfoForBuyer(
                email=receiverEmail,
                time=time,
                amount=amount,
                driverEmail=driverEmail,
                sellerEmail=sellerEmail,
                status='rejected',
                paidAmount=paidAmount,
                notifications_id=notifications_id,
                deliveries_id=deliveries_id,
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content = msg ,
                    code = 200 ,
                    ok = False
                )
            ))
        return ret


    """
    rpc GetAllCompletedDeliveryInfoForBuyerAdm(MessageQuery) returns (AllCompletedDeliveryInfoForBuyer) {}
    """
    def GetAllCompletedDeliveryInfoForBuyerAdm(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        ret = vedbjorn_pb2.AllCompletedDeliveryInfoForBuyer()
        if not self.let_the_client_in(context) :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )
            return ret

        buffer_routes : dict = {}
        db = get_db()

        receivers = get_buyrequests()
        for receiver in receivers :

            if len(receiver) <= 1 :
                continue
            receiverEmail = receiver[1].get('email' , '')
            if not receiverEmail :
                continue
            buyreq_name = receiver[0].get('name' , '')
            if not buyreq_name:
                continue

            accepted_deliveres_it = db.insist_on_find('delivery_accept', {'buyer' : receiverEmail})
            for delivery in mpcur(accepted_deliveres_it) :
                driverEmail : str = ''
                sellerEmail : str = ''
                amount : int = 0
                paidAmount : float = 0
                deliveries_id = str(delivery['_id'])
                notifications_id : str = ''
                time = delivery.get('timestamp' , 0)
                if 'notification' in delivery :
                    notifications_id = str(delivery['notification'])
                if 'payment' in delivery :
                    payment = db.insist_on_find_one('vipps_payments_in' , delivery['payment'])
                    if payment and payment.get('status' , '') == 'paid' and 'amount_NOK' in payment :
                        paidAmount = float(payment['amount_NOK'])
                if 'ongoing_route' in delivery :
                    orid = delivery['ongoing_route']
                    if not str(orid) in buffer_routes :
                        ongoing_route = db.insist_on_find_one('ongoing_routes', orid)
                        if ongoing_route :
                            buffer_routes[str(orid)] = ongoing_route
                    if str(orid) in buffer_routes:
                        ongoing_route = buffer_routes[str(orid)]
                        driver = get_user_with_driverequest_name(ongoing_route['driveRequestName'])
                        if driver :
                            driverEmail = driver[0][0]['email']
                        for travel in ongoing_route['route'] :
                            if travel.get('buyRequest' , {}).get('name' , '_') == buyreq_name :
                                sellRequest_name = travel.get('sellRequest' , {}).get('name' , '_')
                                seller = get_user_with_sellrequest_name(sellRequest_name)
                                if seller :
                                    sellerEmail = seller[0][0]['email']
                                amount = travel.get('loaded_before' , 0) - travel.get('loaded_after' , 0)
                                break
                    else:
                        continue
                ret.deliveries.append(vedbjorn_pb2.CompletedDeliveryInfoForBuyer(
                    email=receiverEmail,
                    time=time,
                    amount=amount,
                    driverEmail=driverEmail,
                    sellerEmail=sellerEmail,
                    status='accepted',
                    paidAmount=paidAmount,
                    notifications_id=notifications_id,
                    deliveries_id=deliveries_id
                ))

            rejected_deliveres_it = db.insist_on_find('delivery_rejections', {'buyer': receiverEmail})
            for delivery in mpcur(rejected_deliveres_it) :
                driverEmail : str = ''
                sellerEmail : str = ''
                amount : int = 0
                paidAmount : float = 0
                deliveries_id = str(delivery['_id'])
                notifications_id : str = ''
                time = delivery.get('timestamp' , 0)
                if 'notification' in delivery :
                    notifications_id = str(delivery['notification'])
                if 'payment' in delivery :
                    payment = db.insist_on_find_one('vipps_payments_in' , delivery['payment'])
                    if payment and payment.get('status' , '') == 'paid' and 'amount_NOK' in payment :
                        paidAmount = float(payment['amount_NOK'])
                if 'ongoing_route' in delivery :
                    orid = delivery['ongoing_route']
                    if not str(orid) in buffer_routes :
                        ongoing_route = db.insist_on_find_one('ongoing_routes', orid)
                        if ongoing_route :
                            buffer_routes[str(orid)] = ongoing_route
                    if str(orid) in buffer_routes:
                        ongoing_route = buffer_routes[str(orid)]
                        driver = get_user_with_driverequest_name(ongoing_route['driveRequestName'])
                        if driver :
                            driverEmail = driver[0][0]['email']
                        for travel in ongoing_route['route'] :
                            if travel.get('buyRequest' , {}).get('name' , '_') == buyreq_name :
                                sellRequest_name = travel.get('sellRequest' , {}).get('name' , '_')
                                seller = get_user_with_sellrequest_name(sellRequest_name)
                                if seller :
                                    sellerEmail = seller[0][0]['email']
                                amount = travel.get('loaded_before' , 0) - travel.get('loaded_after' , 0)
                                break
                    else:
                        continue

                msg : str = 'Decline-reason(s) :\n'
                if delivery.get('wrongAmount', False) == True :
                    msg = msg + '* : Wrong Amount\n'
                if delivery.get('wrongPrice', False) == True :
                    msg = msg + '* : Wrong Price\n'
                if delivery.get('wrongQuality', False) == True :
                    msg = msg + '* : Wrong Quality\n'
                if delivery.get('customMessage' , '') != '' :
                    msg = msg + 'Message : ' + delivery['customMessage']

                ret.deliveries.append(vedbjorn_pb2.CompletedDeliveryInfoForBuyer(
                    email=receiverEmail,
                    time=time,
                    amount=amount,
                    driverEmail=driverEmail,
                    sellerEmail=sellerEmail,
                    status='rejected',
                    paidAmount=paidAmount,
                    notifications_id=notifications_id,
                    deliveries_id=deliveries_id,
                    info=vedbjorn_pb2.GeneralInfoMessage(
                        content = msg ,
                        code = 200 ,
                        ok = False
                    )
                ))
        return ret


    """
    rpc GetDeliveryReceipt(DBQuery) returns (File) {}
    
    message DBQuery {
      string object_id = 1;
      string collection_name = 2;
      string attribute_name = 3;
      string attribute_value = 4;
      uint32 index = 5;
      GeneralInfoMessage info = 6;
    };
    
    message File {
      bytes bytes = 1;
      string media_type = 2;
      uint64 num_bytes = 3;
      GeneralInfoMessage info = 4;
    };
    """
    def GetDeliveryReceipt(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        ret = vedbjorn_pb2.File()
        if not self.let_the_client_in(context) :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )
            return ret

        object_id = request.object_id
        db = get_db()
        delivery_obj = db.insist_on_find_one('delivery_accept' , object_id)

        if not delivery_obj :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content = 'Finalized delivery not found' ,
                code = 404 ,
                ok = False
            )
            return ret
        if not 'receipt' in delivery_obj :
            ongoing_route = db.insist_on_find_one('ongoing_routes' , delivery_obj['ongoing_route'])
            if not ongoing_route :
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Associated route not found',
                    code=404,
                    ok=False
                )
                return ret
            payment = db.insist_on_find_one('vipps_payments_in', delivery_obj['payment'])
            if not payment :
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Payment not found',
                    code=404,
                    ok=False
                )
                return ret
            notification = db.insist_on_find_one('notifications' , delivery_obj['notification'])
            if not notification :
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Notification not found',
                    code=404,
                    ok=False
                )
                return ret
            delivery_proof_doc = db.insist_on_find_one('deliveries' , notification['ref_id'])
            if not delivery_proof_doc :
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Delivery specification not found',
                    code=404,
                    ok=False
                )
                return ret

            user_graph = get_user_with_email(notification['email'])
            userObj = user_graph[0][0]

            driver_graph = get_user_with_driverequest_name(ongoing_route['driveRequestName'])
            driverObj = driver_graph[0][0]

            seller_graph = get_user_with_sellrequest_name(delivery_proof_doc['sellRequest']['name'])
            sellerObj = seller_graph[0][0]

            pdf = FPDF()
            pdf.add_page()
            pdf.image('./bear_less_padded.png', 5, 5, 20, 20)
            pdf.set_font('helvetica' , 'B', 20)
            pdf.cell(80)
            pdf.cell(30, 10, 'Kvittering', align='C', new_y='NEXT', new_x='LMARGIN')

            utc_time = datetime.datetime.utcfromtimestamp(payment['calc_time'])
            NOR_TIME = pytz.timezone('Europe/Oslo')
            pdf.ln(50)
            pdf.set_font('helvetica', '', 16)
            pdf.cell(w=None, h=None, txt='Tidspunkt : ' + utc_time.astimezone(NOR_TIME).strftime('%d-%m-%Y %H:%M:%S')
                     ,align='L',new_y='NEXT', new_x = 'LMARGIN')

            inc_mva = payment['amount_NOK']
            womva = round(inc_mva / 1.25 , 2)
            mva = inc_mva - womva
            pdf.cell(w=None, h=None, txt='Betalt : ' + str(inc_mva) + ' Kr. (' + str(womva) + ' Kr. + ' + str(mva) + ' Kr. MVA)'
                     , align='L', new_y='NEXT', new_x='LMARGIN')

            pdf.cell(w=None, h=None, txt='Betalt av : ' + userObj['name'] + ' (' + userObj['email'] + ')' , align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Levert av : ' + driverObj['name'] + ' (' + driverObj['email'] + ')', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Solgt av : ' + sellerObj['name'] + ' (' + sellerObj['email'] + ')', align='L', new_y='NEXT', new_x='LMARGIN')

            order_id = payment.get('vipps_order_id' , '')
            if order_id :
                pdf.cell(w=None, h=None, txt='Ordre Nummer : ' + order_id, align='L', new_y='NEXT', new_x='LMARGIN')

            pdf.cell(w=None, h=None,
                     txt='Varer : ' + str(delivery_proof_doc['loaded_before'] - delivery_proof_doc['loaded_after']) + ' X 25L sekker med tørrved'
                     , align='L', new_y='NEXT', new_x='LMARGIN')

            pdf.ln(10)

            try:
                with tempfile.TemporaryDirectory() as tmpdirname :
                    path = tmpdirname + '/tmp.jpeg'
                    with open(path, 'wb') as _f:
                        imgproof = db.insist_on_get_filecontent_id(delivery_proof_doc['meta']['file'])
                        _f.write(imgproof)
                        _f.close()
                    img = Image.open(path)
                    available_w = math.floor(pdf.w - pdf.r_margin - pdf.l_margin) - 10
                    pdf.cell(w=None, h=None, txt='Bilde av leveransen :', align='L', new_y='NEXT', new_x='LMARGIN')
                    pdf.image(img, w=available_w)
                    pdf.ln(20)
            except Exception as e:
                print('WARNING : This receipt will be without image, it failed to load because :\n', e)

            pdf.set_font('helvetica', '', 10)
            pdf.cell(w=None, h=None, txt='ID : ' + str(delivery_obj['_id']), align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Handelen ble organisert av vedbjorn.no, en tjeneste levert av :', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Vedbjørn AS , Org.nr 929.350.790', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Forretningsadresse :  Adalsveien 1B, 3185 Skoppum', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Kontakt : Stian Broen (stian@vedbjorn.no)', align='L', new_y='NEXT', new_x='LMARGIN')
            #pdf.output('./TEST.pdf')

            receipt = bytes(pdf.output())
            filename = 'receipt_' + str(delivery_obj['_id']) + '.pdf'
            meta: dict = {
                'Content-Type': 'application/pdf',
                'filename': filename,
                'collection': 'delivery_accept',
                'document': delivery_obj['_id']}
            file_id = db.insist_on_insert_file(receipt, filename, 'pdf', meta)
            db.insist_on_update_one(delivery_obj, 'delivery_accept', 'receipt' , file_id)
        else:
            fileObj_id = delivery_obj['receipt']
            fileObj = db.insist_on_find_one('files.files', fileObj_id)
            filename = fileObj['meta']['filename']
            receipt = db.insist_on_get_filecontent(filename)

        return vedbjorn_pb2.File(
            bytes = receipt ,
            media_type = 'application/pdf' ,
            num_bytes = len(receipt) ,
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = '' ,
                code = 200,
                ok = True))

    """
    rpc GetDeliveryReceiptAdm(DBQuery) returns (File) {}
    """
    def GetDeliveryReceiptAdm(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        ret = vedbjorn_pb2.File()
        if not self.let_the_client_in(context) :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )
            return ret

        object_id = request.object_id
        ismva = str(request.attribute_value).lower() == 'true'
        db = get_db()
        delivery_obj = db.insist_on_find_one('delivery_accept' , object_id)

        if not delivery_obj :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content = 'Finalized delivery not found' ,
                code = 404 ,
                ok = False
            )
            return ret
        if not 'receipt' in delivery_obj or not ismva:
            ongoing_route = db.insist_on_find_one('ongoing_routes' , delivery_obj['ongoing_route'])
            if not ongoing_route :
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Associated route not found',
                    code=404,
                    ok=False
                )
                return ret
            payment = db.insist_on_find_one('vipps_payments_in', delivery_obj['payment'])
            if not payment :
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Payment not found',
                    code=404,
                    ok=False
                )
                return ret
            notification = db.insist_on_find_one('notifications' , delivery_obj['notification'])
            if not notification :
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Notification not found',
                    code=404,
                    ok=False
                )
                return ret
            delivery_proof_doc = db.insist_on_find_one('deliveries' , notification['ref_id'])
            if not delivery_proof_doc :
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Delivery specification not found',
                    code=404,
                    ok=False
                )
                return ret

            user_graph = get_user_with_email(notification['email'])
            userObj = user_graph[0][0]

            driver_graph = get_user_with_driverequest_name(ongoing_route['driveRequestName'])
            driverObj = driver_graph[0][0]

            seller_graph = get_user_with_sellrequest_name(delivery_proof_doc['sellRequest']['name'])
            sellerObj = seller_graph[0][0]

            pdf = FPDF()
            pdf.add_page()
            pdf.image('./bear_less_padded.png', 5, 5, 20, 20)
            pdf.set_font('helvetica' , 'B', 20)
            pdf.cell(80)
            pdf.cell(30, 10, 'Kvittering', align='C', new_y='NEXT', new_x='LMARGIN')

            utc_time = datetime.datetime.utcfromtimestamp(payment['calc_time'])
            NOR_TIME = pytz.timezone('Europe/Oslo')
            pdf.ln(50)
            pdf.set_font('helvetica', '', 16)
            pdf.cell(w=None, h=None, txt='Tidspunkt : ' + utc_time.astimezone(NOR_TIME).strftime('%d-%m-%Y %H:%M:%S')
                     ,align='L',new_y='NEXT', new_x = 'LMARGIN')

            if ismva :
                inc_mva = payment['amount_NOK']
                womva = round(inc_mva / 1.25 , 2)
                mva = inc_mva - womva
            else :
                inc_mva = payment['amount_NOK']
                womva = inc_mva
                mva = 0

            pdf.cell(w=None, h=None, txt='Betalt : ' + str(inc_mva) + ' Kr. (' + str(womva) + ' Kr. + ' + str(mva) + ' Kr. MVA)'
                     , align='L', new_y='NEXT', new_x='LMARGIN')

            pdf.cell(w=None, h=None, txt='Betalt av : ' + userObj['name'] + ' (' + userObj['email'] + ')' , align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Levert av : ' + driverObj['name'] + ' (' + driverObj['email'] + ')', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Solgt av : ' + sellerObj['name'] + ' (' + sellerObj['email'] + ')', align='L', new_y='NEXT', new_x='LMARGIN')

            order_id = payment.get('vipps_order_id' , '')
            if order_id :
                pdf.cell(w=None, h=None, txt='Ordre Nummer : ' + order_id, align='L', new_y='NEXT', new_x='LMARGIN')

            pdf.cell(w=None, h=None,
                     txt='Varer : ' + str(delivery_proof_doc['loaded_before'] - delivery_proof_doc['loaded_after']) + ' X 25L sekker med tørrved'
                     , align='L', new_y='NEXT', new_x='LMARGIN')

            pdf.ln(10)

            try:
                with tempfile.TemporaryDirectory() as tmpdirname :
                    path = tmpdirname + '/tmp.jpeg'
                    with open(path, 'wb') as _f:
                        imgproof = db.insist_on_get_filecontent_id(delivery_proof_doc['meta']['file'])
                        _f.write(imgproof)
                        _f.close()
                    img = Image.open(path)
                    available_w = math.floor(pdf.w - pdf.r_margin - pdf.l_margin) - 10
                    pdf.cell(w=None, h=None, txt='Bilde av leveransen :', align='L', new_y='NEXT', new_x='LMARGIN')
                    pdf.image(img, w=available_w)
                    pdf.ln(20)
            except Exception as e:
                print('WARNING : This receipt will be without image, it failed to load because :\n', e)

            pdf.set_font('helvetica', '', 10)
            pdf.cell(w=None, h=None, txt='ID : ' + str(delivery_obj['_id']), align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Handelen ble organisert av vedbjorn.no, en tjeneste levert av :', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Vedbjørn AS , Org.nr 929.350.790', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Forretningsadresse :  Adalsveien 1B, 3185 Skoppum', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Kontakt : Stian Broen (stian@vedbjorn.no)', align='L', new_y='NEXT', new_x='LMARGIN')
            #pdf.output('./TEST.pdf')

            receipt = bytes(pdf.output())
            if ismva :
                filename = 'receipt_' + str(delivery_obj['_id']) + '.pdf'
                meta: dict = {
                    'Content-Type': 'application/pdf',
                    'filename': filename,
                    'collection': 'delivery_accept',
                    'document': delivery_obj['_id']}
                file_id = db.insist_on_insert_file(receipt, filename, 'pdf', meta)
                db.insist_on_update_one(delivery_obj, 'delivery_accept', 'receipt' , file_id)
        else:
            fileObj_id = delivery_obj['receipt']
            fileObj = db.insist_on_find_one('files.files', fileObj_id)
            filename = fileObj['meta']['filename']
            receipt = db.insist_on_get_filecontent(filename)

        return vedbjorn_pb2.File(
            bytes = receipt ,
            media_type = 'application/pdf' ,
            num_bytes = len(receipt) ,
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = '' ,
                code = 200,
                ok = True))



    """
    rpc GetFinishedRouteInvoice(DBQuery) returns (File) {}
    """
    def GetFinishedRouteInvoice(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        ret = vedbjorn_pb2.File()
        if not self.let_the_client_in(context) :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )
            return ret

        object_id = request.object_id
        email = request.attribute_value
        drivereqGraph = get_driverequests_with_email(email)

        if not drivereqGraph:
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='Could not find a driverequest associated with this user',
                code=404,
                ok=False
            )
            return ret

        driverObj = drivereqGraph[0][0]
        driver_name = driverObj.get('name', '')
        db = get_db()

        wrapup = db.insist_on_find_one('wrapup_routes', object_id)
        if not wrapup :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content = 'Route wrapup document not found' ,
                code = 404 ,
                ok = False
            )
            return ret

        ongoing_route = db.insist_on_find_one_q('ongoing_routes', {
            '_id': wrapup['ongoing_route'],
            'driveRequestName': driver_name
        })
        if not ongoing_route:
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='Could not find ongoing_routes associated with this user and id from wrapup',
                code=404,
                ok=False
            )
            return ret

        payment_obj = db.insist_on_find_one_q('vipps_payments_out' , {
            'receiving_user.email' : email ,
            'ref.ongoing_route' : ongoing_route['_id']
        })
        if not payment_obj or not 'invoice_id' in payment_obj :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='Could not find the invoice',
                code=404,
                ok=False
            )
            return ret

        fileObj_id = payment_obj['invoice_id']
        fileObj = db.insist_on_find_one('files.files', fileObj_id)
        filename = fileObj['meta']['filename']
        receipt = db.insist_on_get_filecontent(filename)

        return vedbjorn_pb2.File(
            bytes=receipt,
            media_type='application/pdf',
            num_bytes=len(receipt),
            info=vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=200,
                ok=True))

    """
    rpc GetFinishedRouteReceipt(DBQuery) returns (File) {}
    """
    def GetFinishedRouteReceipt(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        ret = vedbjorn_pb2.File()
        if not self.let_the_client_in(context) :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )
            return ret

        object_id = request.object_id
        email = request.attribute_value
        drivereqGraph = get_driverequests_with_email(email)
        ret = vedbjorn_pb2.File()
        if not drivereqGraph :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='Could not find a driverequest associated with this user',
                code=404,
                ok=False
            )
            return ret

        driverObj = drivereqGraph[0][0]
        driver_name = driverObj.get('name', '')
        db = get_db()
        wrapup = db.insist_on_find_one('wrapup_routes', object_id)
        if not wrapup :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content = 'Route wrapup document not found' ,
                code = 404 ,
                ok = False
            )
            return ret

        ongoing_route = db.insist_on_find_one_q('ongoing_routes' , {
            '_id' : wrapup['ongoing_route'] ,
            'driveRequestName' : driver_name
        })
        if not ongoing_route :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='Could not find ongoing_routes associated with this user and id from wrapup',
                code=404,
                ok=False
            )
            return ret

        if not 'receipt' in wrapup:
            ongoing_route = db.insist_on_find_one_q('ongoing_routes', {'wrapup': ObjectId(object_id)})
            if not ongoing_route:
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Associated ongoing route not found',
                    code=404,
                    ok=False
                )
                return ret

            pdf = FPDF()
            pdf.add_page()
            pdf.image('./bear_less_padded.png', 5, 5, 20, 20)
            pdf.set_font('helvetica', 'B', 20)
            pdf.cell(80)
            pdf.cell(30, 10, 'Gjennomført oppdrag', align='C', new_y='NEXT', new_x='LMARGIN')

            pdf.ln(20)
            pdf.set_font('helvetica', '', 16)

            driver_graph = get_user_with_driverequest_name(ongoing_route['driveRequestName'])
            driverObj = driver_graph[0][0]

            pdf.cell(w=None, h=None, txt='Gjennomført av : ' + driverObj['name'] + ' (' + driverObj['email'] + ')'
                     , align='L', new_y='NEXT', new_x='LMARGIN')

            began = wrapup['began']
            NOR_TIME = pytz.timezone('Europe/Oslo')
            pdf.cell(w=None, h=None, txt='Oppstart : ' + began.astimezone(NOR_TIME).strftime('%d-%m-%Y %H:%M:%S')
                     , align='L', new_y='NEXT', new_x='LMARGIN')

            ended = wrapup['ended']
            NOR_TIME = pytz.timezone('Europe/Oslo')
            pdf.cell(w=None, h=None, txt='Ferdig : ' + ended.astimezone(NOR_TIME).strftime('%d-%m-%Y %H:%M:%S')
                     , align='L', new_y='NEXT', new_x='LMARGIN')

            pdf.ln(5)

            # inc_mva = wrapup['total_income_from_sales_paid']
            # womva = round(inc_mva / 1.25, 2)
            # mva = inc_mva - womva
            # pdf.cell(w=None, h=None,
            #          txt='Salgsinntekter : ' + str(inc_mva) + ' Kr. (' + str(womva) + ' Kr. + ' + str(mva) + ' Kr. MVA)'
            #          , align='L', new_y='NEXT', new_x='LMARGIN')
            #
            # payObj = db.insist_on_find_one_q('vipps_payments_out' , {
            #     'ref.ongoing_route' : ongoing_route['_id'] ,
            #     'target': 'driver'})
            #
            # inc_mva = payObj['amount_NOK']
            # womva = round(inc_mva / 1.25, 2)
            # mva = inc_mva - womva
            # pdf.cell(w=None, h=None,
            #          txt='Inntekt for gjennomført kjøreoppdrag : ' + str(inc_mva) + ' Kr. (' + str(womva) + ' Kr. + ' + str(mva) + ' Kr. MVA)'
            #          , align='L', new_y='NEXT', new_x='LMARGIN')
            #
            # sellpayit = db.insist_on_find('vipps_payments_out' , {
            #     'ref.ongoing_route' : ongoing_route['_id'] ,
            #     'target': 'seller'})
            #
            # inc_mva = 0
            # for sellpay in mpcur(sellpayit) :
            #     inc_mva = inc_mva + sellpay.get('amount_NOK' , 0)
            #
            # womva = round(inc_mva / 1.25, 2)
            # mva = inc_mva - womva
            # pdf.cell(w=None, h=None,
            #          txt='Inntekt for vedselgere : ' + str(inc_mva) + ' Kr. (' + str(
            #              womva) + ' Kr. + ' + str(mva) + ' Kr. MVA)'
            #          , align='L', new_y='NEXT', new_x='LMARGIN')

            index: int = 0
            positions : list = []
            max_lat : float = -999
            max_lng : float = -999
            min_lat : float = 999
            min_lng : float = 999
            for visit in ongoing_route.get('route', []):
                index = index + 1
                txt: str = str(index)
                # if visit.get('type', '') == 'pickup':
                #     txt = str(index) + '_Henting'
                # elif visit.get('type', '') == 'delivery':
                #     txt = str(index) + '_Leveranse'
                # elif visit.get('type', '') == 'return':
                #     txt = str(index) + '_Retur'
                # elif visit.get('type', '') == 'visit_start':
                #     txt = str(index) + '_Oppstart'
                # elif visit.get('type', '') == 'visit_end':
                #     txt = str(index) + '_Ferdig'
                # else:
                #     txt = str(index) + '_Ubestemt'
                lat = visit['to']['lat']
                lng = visit['to']['lng']
                positions.append({
                    'lat': lat,
                    'lng': lng,
                    'text': txt
                })
                if lat > max_lat :
                    max_lat = lat
                if lat < min_lat :
                    min_lat = lat
                if lng > max_lng :
                    max_lng = lng
                if lng < min_lng :
                    min_lng = lng

            avg_lat = (max_lat + min_lat) / 2
            avg_lng = (max_lng + min_lng) / 2
            center : str = str(avg_lat) + ',' + str(avg_lng)

            GOOGLE_MAPS_API_KEY = 'AIzaSyC7u-9hq27kASmDbVRvsc14jzsgqBsjp90'

            url = 'https://maps.googleapis.com/maps/api/staticmap' + \
                  '?center=' + center + \
                  '&key=' + GOOGLE_MAPS_API_KEY + \
                  '&size=400x400'

            for i in range(0, len(positions)) :
                url = url + '&markers=color:red%7Clabel:' + positions[i]['text'] + \
                    '%7C' + str(positions[i]['lat']) + ',' + str(positions[i]['lng'])

            r = requests.get(url)
            if r.status_code == 200 :
                map_content = r.content
                with tempfile.TemporaryDirectory() as tmpdirname:
                    path = tmpdirname + '/map.png'
                    with open(path, 'wb') as _f:
                        _f.write(map_content)
                        _f.close()
                    img = Image.open(path)
                    pdf.ln(7)
                    pdf.cell(w=None, h=None, txt='Kart over oppdraget', align='L', new_y='NEXT', new_x='CENTER')
                    pdf.ln(2)
                    pdf.image(img)
                    pdf.add_page()

            pdf.ln(7)
            pdf.cell(w=None, h=None,txt='Bilder fra stopp-punkter', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.ln(4)
            index : int = 0
            pdf.set_font('helvetica', '', 10)
            y = 0
            for visit in ongoing_route.get('route' , []) :
                index = index + 1
                if 'visited' in visit :
                    visitObj = None
                    txt : str = ''
                    if visit.get('type' , '') == 'pickup' :
                        visitObj = db.insist_on_find_one('pickups' , visit['visited'])
                        txt = str(index) + ' : Henting'
                    elif visit.get('type', '') == 'delivery':
                        visitObj = db.insist_on_find_one('deliveries', visit['visited'])
                        txt = str(index) + ' : Leveranse'
                    elif visit.get('type', '') == 'return':
                        visitObj = db.insist_on_find_one('returns', visit['visited'])
                        txt = str(index) + ' : Return'
                    elif visit.get('type', '') == 'visit_start':
                        visitObj = db.insist_on_find_one('visit_start', visit['visited'])
                        txt = str(index) + ' : Oppstart'
                    elif visit.get('type', '') == 'visit_end':
                        visitObj = db.insist_on_find_one('visit_end', visit['visited'])
                        txt = str(index) + ' : Ferdig'
                    if visitObj :
                        with tempfile.TemporaryDirectory() as tmpdirname:
                            path = tmpdirname + '/tmp.jpeg'
                            with open(path, 'wb') as _f:
                                imgproof = db.insist_on_get_filecontent_id(visitObj['meta']['file'])
                                _f.write(imgproof)
                                _f.close()
                            img = Image.open(path)

                            available_w = math.floor(pdf.w - pdf.r_margin - pdf.l_margin) - 10
                            left_side = ((index-1) % 2) == 0
                            if left_side :
                                x = pdf.l_margin
                                y = pdf.get_y()
                            else:
                                x = pdf.w / 2
                                pdf.set_y(y)

                            pdf.set_x(x)
                            pdf.cell(w=None, h=None,txt=txt, align='L', new_y='NEXT', new_x='LMARGIN')
                            pdf.image(img, w=available_w/2, x=x)

            pdf.ln(7)
            pdf.cell(w=None, h=None, txt='ID : ' + str(wrapup['_id']), align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Handelen ble organisert av vedbjorn.no, en tjeneste levert av :', align='L',
                     new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Vedbjørn AS , Org.nr 929.350.790', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Forretningsadresse :  Adalsveien 1B, 3185 Skoppum', align='L', new_y='NEXT',
                     new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Kontakt : Stian Broen (stian@vedbjorn.no)', align='L', new_y='NEXT',
                     new_x='LMARGIN')
            # pdf.output('./TEST.pdf')

            receipt = bytes(pdf.output())
            filename = 'receipt_' + str(wrapup['_id']) + '.pdf'
            meta: dict = {
                'Content-Type': 'application/pdf',
                'filename': filename,
                'collection': 'wrapup_routes',
                'document': wrapup['_id']}
            file_id = db.insist_on_insert_file(receipt, filename, 'pdf', meta)
            db.insist_on_update_one(wrapup, 'wrapup_routes', 'receipt', file_id)

        else:
            fileObj_id = wrapup['receipt']
            fileObj = db.insist_on_find_one('files.files', fileObj_id)
            filename = fileObj['meta']['filename']
            receipt = db.insist_on_get_filecontent(filename)

        return vedbjorn_pb2.File(
            bytes = receipt ,
            media_type = 'application/pdf' ,
            num_bytes = len(receipt) ,
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = '' ,
                code = 200,
                ok = True))

    """
    rpc GetNewSellerDealInfoList(UserContactInfo) returns (SellerDealInfoList) {}
    
    message SellerDealInfo {
      string planned_routes_id = 1;
      string driverName = 2;
      string driverEmail = 3;
      uint32 numBags = 4;
      double earningEstimate = 5;
      bool accepted = 6;
      GeneralInfoMessage info = 7;
    };
    
    message SellerDealInfoList {
      repeated SellerDeal deals = 1;
      GeneralInfoMessage info = 2;
    };
    """
    def GetNewSellerDealInfoList(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.SellerDealInfoList(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        sellerEmail = request.email_address
        sellreq = get_sellrequests_with_email(sellerEmail)
        if not sellreq :
            return vedbjorn_pb2.SellerDealInfoList(
                info = vedbjorn_pb2.GeneralInfoMessage(
                    content = 'sellreq not found' ,
                    code = 404 ,
                    ok = False
                )
            )
        sellObj = sellreq[0][0]
        db = get_db()
        pr_it = db.insist_on_find('planned_routes')
        ret = vedbjorn_pb2.SellerDealInfoList()
        for pr in mpcur(pr_it) :
            for name, info in pr.get('deals' , {}).items():
                if name == sellObj['name'] :
                    driverGraph = get_user_with_driverequest_name(pr['driveRequestName'])
                    if not driverGraph :
                        continue
                    driverObj = driverGraph[0][0]
                    ret.deals.append(vedbjorn_pb2.SellerDealInfo(
                        planned_routes_id = str(pr['_id']) ,
                        driverName = driverObj['name'] ,
                        driverEmail = driverObj['email'] ,
                        numBags = info['number_of_bags_sold'] ,
                        earningEstimate = info['number_of_bags_sold'] * 100,  # TODO : Retrieve real price estimate
                        accepted = info.get('accepted', False) ,
                        calc_time = pr['calc_time']
                    ))
        return ret

    """
    rpc GetNewSellerDealAccept(SellerDealInfo) returns (GeneralInfoMessage) {}
    """
    def GetNewSellerDealAccept(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )

        sellerEmail = request.sellerEmail
        pr_id = request.planned_routes_id
        accept = request.accept
        reason = request.reason

        db = get_db()
        pr = db.insist_on_find_one('planned_routes', pr_id)
        if not pr :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Planned route not found',
                code=404,
                ok=False
            )

        sellreq_graph = get_sellrequests_with_email(sellerEmail)
        if not sellreq_graph :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Sellrequest not found',
                code=404,
                ok=False
            )
        sellReq = sellreq_graph[0][0]
        found = False
        for sellName, deal in pr.get('deals', {}).items() :
            if sellName == sellReq['name'] :
                if 'accept' in deal :
                    if deal['acceot'] == True :
                        return vedbjorn_pb2.GeneralInfoMessage(
                            content='Deal has already been accepted',
                            code=400,
                            ok=False
                        )
                    else:
                        return vedbjorn_pb2.GeneralInfoMessage(
                            content='Deal has already been rejected',
                            code=400,
                            ok=False
                        )

                deal['accepted'] = accept
                db.insist_on_update_one(pr, 'planned_routes', 'deals', pr['deals'])
                found = True
                break
        if not found :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='Deal not found',
                code=404,
                ok=False
            )
        pr = db.insist_on_find_one('planned_routes', pr_id)
        num_sellers = 0
        num_sellers_accepted = 0
        for sellName, deal in pr.get('deals', {}).items():
            num_sellers = num_sellers + 1
            if deal.get('accepted', False) == True :
                num_sellers_accepted = num_sellers_accepted + 1
        db.insist_on_update_one(pr, 'planned_routes', 'num_sellers', num_sellers)
        db.insist_on_update_one(pr, 'planned_routes', 'num_sellers_accepted', num_sellers_accepted)
        if num_sellers > 0 and num_sellers == num_sellers_accepted :
            driver_graph = get_user_with_driverequest_name(pr.get('driveRequestName' , ''))
            if driver_graph :
                db.insist_on_insert_one('notifications', {
                    'email': driver_graph[0][0]['email'],
                    'timestamp': datetime.datetime.utcnow().timestamp(),
                    'planned_routes': pr_id,
                    'contentType': 'new assignment',
                    'status': 'new',
                    'text': 'Et oppdrag er klart for igangsetting.'
                })
        return vedbjorn_pb2.GeneralInfoMessage(
            content = '' ,
            code = 200 ,
            ok = True
        )

    """
    rpc GetOngoingSellerDealInfoList(UserContactInfo) returns (SellerDealInfoList) {}
    """
    def GetOngoingSellerDealInfoList(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.SellerDealInfoList(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        sellerEmail = request.email_address
        sellreq = get_sellrequests_with_email(sellerEmail)
        if not sellreq:
            return vedbjorn_pb2.SellerDealInfoList(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='sellreq not found',
                    code=404,
                    ok=False
                )
            )
        sellObj = sellreq[0][0]
        db = get_db()
        pr_it = db.insist_on_find('ongoing_routes' , {'wrapup' : None})
        ret = vedbjorn_pb2.SellerDealInfoList()
        for pr in mpcur(pr_it) :
            for name, info in pr.get('deals' , {}).items():
                if name == sellObj['name'] :
                    driverGraph = get_user_with_driverequest_name(pr['driveRequestName'])
                    if not driverGraph :
                        continue
                    driverObj = driverGraph[0][0]
                    ret.deals.append(vedbjorn_pb2.SellerDealInfo(
                        planned_routes_id = str(pr['_id']) ,
                        driverName = driverObj['name'] ,
                        driverEmail = driverObj['email'] ,
                        numBags = info['number_of_bags_sold'] ,
                        earningEstimate = info['number_of_bags_sold'] * 100,  # TODO : Retrieve real price estimate
                        accepted = info.get('accepted', False) ,
                        calc_time = pr['calc_time']
                    ))
        return ret

    """
    rpc GetCompletedSells(UserContactInfo) returns (SellerDealInfoList) {}
    """
    def GetCompletedSells(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.SellerDealInfoList(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        sellerEmail = request.email_address
        sellreq = get_sellrequests_with_email(sellerEmail)
        if not sellreq:
            return vedbjorn_pb2.SellerDealInfoList(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='sellreq not found',
                    code=404,
                    ok=False
                )
            )
        sellObj = sellreq[0][0]
        db = get_db()

        pay_obj_it = db.insist_on_find('vipps_payments_out', {
            'target' : 'seller' ,
            'receiving_user.name' : sellObj['name']
        })
        ors : dict = {}
        ret = vedbjorn_pb2.SellerDealInfoList()
        for pay_obj in mpcur(pay_obj_it) :
            or_id = str(pay_obj.get('ref' , {}).get('ongoing_route' , ''))
            if or_id and not or_id in ors :
                ong_r = db.insist_on_find_one('ongoing_routes' , or_id)
                if ong_r :
                    ors[or_id] = ong_r

            ong_r = ors.get(or_id, {})
            if not ong_r :
                continue

            num_bags : int = 0
            for sell_name, deal in ong_r.get('deals' , {}).items() :
                if sell_name == sellObj['name'] :
                    num_bags = num_bags + deal.get('number_of_bags_sold' , 0)

            driverGraph = get_user_with_driverequest_name(ong_r['driveRequestName'])
            if not driverGraph:
                continue
            driverObj = driverGraph[0][0]

            ret.deals.append(vedbjorn_pb2.SellerDealInfo(
                planned_routes_id=str(ong_r['_id']),
                driverName=driverObj['name'],
                driverEmail=driverObj['email'],
                numBags=num_bags,
                earningEstimate=pay_obj.get('amount_NOK' , 0) ,
                accepted=pay_obj.get('status', False) == True,
                calc_time=pay_obj['calc_time']
            ))
        return ret

    """
    rpc GetSellsInvoice(DBQuery) returns (File) {}
    """
    def GetSellsInvoice(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        ret = vedbjorn_pb2.File()
        if not self.let_the_client_in(context) :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )
            return ret

        object_id = request.object_id
        seller_email = request.attribute_value
        seller_graph = get_sellrequests_with_email(seller_email)

        if not seller_graph:
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='Seller object not found',
                code=404,
                ok=False
            )
            return ret
        sellObj = seller_graph[0][0]

        db = get_db()
        on_rt = db.insist_on_find_one('ongoing_routes', object_id)

        if not on_rt:
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='Sales-route not found',
                code=404,
                ok=False
            )
            return ret

        pay_obj = db.insist_on_find_one_q('vipps_payments_out' , {
            'receiving_user.name' : sellObj.get('name' , '_') ,
            'ref.ongoing_route' : on_rt['_id']
        })

        if not pay_obj or not 'invoice_id' in pay_obj :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='Invoice not found',
                code=404,
                ok=False
            )
            return ret

        fileObj_id = pay_obj['invoice_id']
        fileObj = db.insist_on_find_one('files.files', fileObj_id)
        filename = fileObj['meta']['filename']
        receipt = db.insist_on_get_filecontent(filename)

        return vedbjorn_pb2.File(
            bytes=receipt,
            media_type='application/pdf',
            num_bytes=len(receipt),
            info=vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=200,
                ok=True))
    """
    rpc GetSellsReceipt(DBQuery) returns (File) {}
    
    message DBQuery {
      string object_id = 1;
      string collection_name = 2;
      string attribute_name = 3;
      string attribute_value = 4;
      uint32 index = 5;
      GeneralInfoMessage info = 6;
    };
    
    message File {
      bytes bytes = 1;
      string media_type = 2;
      uint64 num_bytes = 3;
      GeneralInfoMessage info = 4;
    };
    """
    def GetSellsReceipt(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        ret = vedbjorn_pb2.File()
        if not self.let_the_client_in(context) :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=401,
                ok=False
            )
            return ret

        object_id = request.object_id
        seller_email = request.attribute_value
        seller_graph = get_sellrequests_with_email(seller_email)

        if not seller_graph :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content='Seller object not found',
                code=404,
                ok=False
            )
            return ret
        sellObj = seller_graph[0][0]

        db = get_db()
        on_rt = db.insist_on_find_one('ongoing_routes' , object_id)

        if not on_rt :
            ret.info = vedbjorn_pb2.GeneralInfoMessage(
                content = 'Sales-route not found' ,
                code = 404 ,
                ok = False
            )
            return ret

        already_receipt = db.insist_on_find_one_q('files.files' , {
            'meta.ongoing_route' : on_rt['_id'] ,
            'meta.Content-Type' : 'application/pdf' ,
            'meta.seller_email' : seller_email
        })

        if not already_receipt :

            payment = db.insist_on_find_one_q('vipps_payments_out', {
                'ref.ongoing_route' : on_rt['_id'] ,
                'receiving_user.name' : sellObj['name'] ,
                'target' : 'seller'
            })

            if not payment :
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Payment not found',
                    code=404,
                    ok=False
                )
                return ret

            notification = db.insist_on_find_one_q('notifications', {
                'ongoing_routes' : on_rt['_id'] ,
                'email' : seller_email
            })

            if not notification:
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Notification not found',
                    code=404,
                    ok=False
                )
                return ret

            pickups_proof_doc = db.insist_on_find_one_q('pickups', {
                'ongoing_route' : on_rt['_id'] ,
                'sellRequestName' : sellObj['name']
            })

            if not pickups_proof_doc:
                ret.info = vedbjorn_pb2.GeneralInfoMessage(
                    content='Pickup specification not found',
                    code=404,
                    ok=False
                )
                return ret


            driver_graph = get_user_with_driverequest_name(on_rt['driveRequestName'])
            driverObj = driver_graph[0][0]

            pdf = FPDF()
            pdf.add_page()
            pdf.image('./bear_less_padded.png', 5, 5, 20, 20)
            pdf.set_font('helvetica' , 'B', 20)
            pdf.cell(80)
            pdf.cell(30, 10, 'Salgs-dokumentasjon', align='C', new_y='NEXT', new_x='LMARGIN')

            utc_time = datetime.datetime.utcfromtimestamp(payment['calc_time'])
            NOR_TIME = pytz.timezone('Europe/Oslo')
            pdf.ln(50)
            pdf.set_font('helvetica', '', 16)
            pdf.cell(w=None, h=None, txt='Tidspunkt : ' + utc_time.astimezone(NOR_TIME).strftime('%d-%m-%Y %H:%M:%S')
                     ,align='L',new_y='NEXT', new_x = 'LMARGIN')

            # inc_mva = payment['amount_NOK']
            # womva = round(inc_mva / 1.25 , 2)
            # mva = inc_mva - womva
            # pdf.cell(w=None, h=None, txt='Betalt : ' + str(inc_mva) + ' Kr. (' + str(womva) + ' Kr. + ' + str(mva) + ' Kr. MVA)'
            #          , align='L', new_y='NEXT', new_x='LMARGIN')
            #
            # pdf.cell(w=None, h=None, txt='Levert av : ' + driverObj['name'] + ' (' + driverObj['email'] + ')', align='L', new_y='NEXT', new_x='LMARGIN')
            # pdf.cell(w=None, h=None, txt='Solgt av : ' + sellObj['name'] + ' (' + seller_email + ')', align='L', new_y='NEXT', new_x='LMARGIN')
            #
            # pdf.cell(w=None, h=None,
            #          txt='Varer : ' + str(pickups_proof_doc['loaded_after'] - pickups_proof_doc['loaded_before']) + ' X 25L sekker med tørrved'
            #          , align='L', new_y='NEXT', new_x='LMARGIN')
            #
            # pdf.ln(10)

            pdf.cell(w=None, h=None, txt='Bilde av leveransen :' , align='L', new_y='NEXT', new_x='LMARGIN')
            with tempfile.TemporaryDirectory() as tmpdirname :
                path = tmpdirname + '/tmp.jpeg'
                with open(path, 'wb') as _f:
                    imgproof = db.insist_on_get_filecontent_id(pickups_proof_doc['meta']['file'])
                    _f.write(imgproof)
                    _f.close()
                img = Image.open(path)
                available_w = math.floor(pdf.w - pdf.r_margin - pdf.l_margin) - 10
                pdf.image(img, w=available_w)

            pdf.ln(20)
            pdf.set_font('helvetica', '', 10)
            pdf.cell(w=None, h=None, txt='ID : ' + str(payment['_id']), align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Handelen ble organisert av vedbjorn.no, en tjeneste levert av :', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Vedbjørn AS , Org.nr 929.350.790', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Forretningsadresse :  Adalsveien 1B, 3185 Skoppum', align='L', new_y='NEXT', new_x='LMARGIN')
            pdf.cell(w=None, h=None, txt='Kontakt : Stian Broen (stian@vedbjorn.no)', align='L', new_y='NEXT', new_x='LMARGIN')
            #pdf.output('./TEST.pdf')

            receipt = bytes(pdf.output())
            filename = 'receipt_' + str(payment['_id']) + '.pdf'
            meta: dict = {
                'Content-Type': 'application/pdf',
                'filename': filename,
                'collection': 'delivery_accept',
                'document': payment['_id'] ,
                'seller_email' : seller_email ,
                'ongoing_route' : on_rt['_id']
            }
            file_id = db.insist_on_insert_file(receipt, filename, 'pdf', meta)
            db.insist_on_update_one(payment, 'delivery_accept', 'receipt' , file_id)
        else:
            filename = already_receipt['meta']['filename']
            receipt = db.insist_on_get_filecontent(filename)

        return vedbjorn_pb2.File(
            bytes = receipt ,
            media_type = 'application/pdf' ,
            num_bytes = len(receipt) ,
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = '' ,
                code = 200,
                ok = True))


    """
    rpc GetPaymentInfo(PaymentInfoQuery) returns (PaymentInfo) {}
    
    message PaymentInfo {
      string mongodb_id          = 1;
      string paying_user_email   = 2;
      string paying_user_phone   = 3;
      string receiving_user_name = 4;
      string message_to_payer    = 5;
      string ref_code            = 6;
      string ref_collection      = 7;
      string ref_visit_id        = 8;
      string ref_route_id        = 9;
      string status              = 10;
      float amount_NOK           = 11;
      float calc_time            = 12;
      vipps_order_id             = 13
    };
    
    message PaymentInfoQuery {
      string notification_id = 1;
    };
    """
    def GetPaymentInfo(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        empty_response = vedbjorn_pb2.PaymentInfo(
            mongodb_id          = str('') ,
            paying_user_email   = str('') ,
            paying_user_phone   = str('') ,
            receiving_user_name = str('') ,
            message_to_payer    = str('') ,
            ref_code            = str('') ,
            ref_collection      = str('') ,
            ref_visit_id        = str('') ,
            ref_route_id        = str('') ,
            status              = str('') ,
            amount_NOK          = float(0.0) ,
            calc_time           = float(0.0) ,
            vipps_order_id      = str('') ,
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = str('Resource unavailable') ,
                code    = int(400) ,
                ok      = bool(False)
            )
        )

        if not self.let_the_client_in(context) :
            empty_response.info.code = 401
            empty_response.info.ok = False
            empty_response.info.content = ''
            return empty_response

        ref_id = request.notification_id
        db = get_db()
        if not ref_id or ref_id == 'undefined' :
            """
            This is the situation where the frontend user is progressing from top-level of the buy-meny to get info
            and has perhaps lost the previous state of the application that was holding order-id and those things.
            It means we MIGHT have a delivery that has been paid and that has not been processed
            """
            notifObj = None
            email = request.email
            notif_iter = db.insist_on_find('notifications' , {
                'email' : email,
                'contentType' : 'delivery',
                'ref_collection' : 'deliveries'
            })
            for notifObj in mpcur(notif_iter) :
                """
                TODO : There might be a notification of a delivery, that has according to the payment been paid, but not processed
                with 'PushFeedbackAcceptDelivery'.
                
                So we much search here for that situation. And then from BuyPage, needs to lead into OrderStatus component in the frontend
                so that the user can finalize the transaction

                """
                if not 'ref_id' in notifObj :
                    continue
                delivery = db.insist_on_find_one('deliveries' , notifObj['ref_id'])
                if not delivery or not 'payment_ref' in delivery:
                    continue
                payment = db.insist_on_find_one('vipps_payments_in' , delivery['payment_ref'])

                delivery_accept = db.insist_on_find_one_q('delivery_accept', {
                    'notification': notifObj['_id'],
                    'payment': payment['_id']
                })

                if not delivery_accept and payment.get('status' , '') == 'paid' :
                    #
                    #
                    #####
                    """
                    
                    Special situation : The user paid the delivery from vipps, but did not accept the delivery from the UI
                    explicitly. It could be that he/she just forgot, then reloaded the UI
                    
                    Update relevant documents with the rejection-details
                    """
                    receiverEmail = notifObj['email']
                    ongoing_route = db.insist_on_find_one('ongoing_routes' , delivery['ongoing_route'])

                    accept_ref = db.insist_on_insert_one('delivery_accept', {
                        'buyer': receiverEmail,
                        'notification': notifObj['_id'],
                        'payment': payment['_id'],
                        'ongoing_route': ongoing_route['_id'],
                        'timestamp': datetime.datetime.utcnow().timestamp()
                    })
                    db.insist_on_update_one(notifObj, 'notifications', 'status', 'accepted')
                    db.insist_on_update_one(notifObj, 'notifications', 'accept', accept_ref)
                    db.insist_on_update_one(delivery, 'deliveries', 'accept', accept_ref)
                    if payment:
                        db.insist_on_update_one(payment, 'vipps_payments_in', 'accept', accept_ref)
                    index: int = -1
                    seller_email: str = ''
                    num_bags: int = 0
                    for travel in ongoing_route.get('route', []):
                        index = index + 1
                        if str(travel.get('visited', '')) == str(delivery['_id']):
                            db.insist_on_set_attribute_in_array_at_index(
                                id=ongoing_route['_id'],
                                col='ongoing_routes',
                                index=index,
                                arrayName='route',
                                attributeName='accepted',
                                attributeValue=accept_ref
                            )
                            sellReq_name = travel.get('sellRequest', {}).get('name', '')
                            num_bags = travel.get('loaded_before', 0) - travel.get('loaded_after', 0)
                            sellUser = get_user_with_sellrequest_name(sellReq_name)
                            if sellUser:
                                seller_email = sellUser[0][0]['email']
                            break

                    driver = get_user_with_driverequest_name(ongoing_route['driveRequestName'])
                    db.insist_on_insert_one('notifications', {
                        'email': seller_email,
                        'timestamp': datetime.datetime.utcnow().timestamp(),
                        'contentType': 'accepted',
                        'ref_collection': 'ongoing_routes',
                        'ref_id': ongoing_route['_id'],
                        'amount': index,
                        'status': 'new',
                        'meta': delivery.get('meta', {}),
                        'text': 'En kunde (' + receiverEmail + ') har godkjent en leveranse med ved fra deg.'
                                + str(num_bags) + ' sekker ble levert av ' + driver[0][0]['email'] + '.' +
                                'Det betyr at veden din holder den kvaliteten den skal. Godt jobbet!'
                    })

                    return vedbjorn_pb2.PaymentInfo(
                        mongodb_id=str(payment['_id']),
                        paying_user_email=str(notifObj['email']),
                        paying_user_phone=str(payment['paying_user']['phone']),
                        receiving_user_name=str(payment['receiving_user']),
                        message_to_payer=str(payment['message']),
                        ref_code=str(payment['ref']['the_code']),
                        ref_collection=str(payment['ref']['collection']),
                        ref_visit_id=str(payment['ref']['visit_id']),
                        ref_route_id=str(payment['ref']['route']),
                        status=str(payment['status']),
                        amount_NOK=float(payment['amount_NOK']),
                        calc_time=float(payment['calc_time']),
                        vipps_order_id=str(payment.get('vipps_order_id', '')),
                        info=vedbjorn_pb2.GeneralInfoMessage(
                            content=str(''),
                            code=int(200),
                            ok=bool(True)
                        )
                    )
                    #####
                    #
                    #

            return empty_response

        else:
            notifObj = db.insist_on_find_one_q('notifications' , {'ref_id' : ref_id})
        if not notifObj :
            payment = db.insist_on_find_one('vipps_payments_in', ref_id)
            if not payment :
                return empty_response
            delivery = db.insist_on_find_one(payment['ref']['collection'], payment['ref']['visit_id'])
            if not delivery:
                return empty_response
            notifObj = db.insist_on_find_one('notifications', delivery['notification'])
            if not notifObj:
                return empty_response
        else :
            delivery = db.insist_on_find_one(notifObj['ref_collection'] , ref_id)
            if not delivery :
                return empty_response
            payment = db.insist_on_find_one('vipps_payments_in' , delivery['payment_ref'])

        if not payment :
            return empty_response


        phnum = str(payment['paying_user']['phone'])

        # # TODO : REMOVE :
        # phnum = '99999999'

        if len(phnum) == 8 :
            phnum = '47' + phnum

        return vedbjorn_pb2.PaymentInfo(
            mongodb_id          = str(payment['_id']),
            paying_user_email   = str(notifObj['email']),
            paying_user_phone   = phnum,
            receiving_user_name = str(payment['receiving_user']),
            message_to_payer    = str(payment['message']),
            ref_code            = str(payment['ref']['the_code']),
            ref_collection      = str(payment['ref']['collection']),
            ref_visit_id        = str(payment['ref']['visit_id']),
            ref_route_id        = str(payment['ref']['route']),
            status              = str(payment['status']),
            amount_NOK          = float(payment['amount_NOK']),
            calc_time           = float(payment['calc_time']),
            vipps_order_id      = str(payment.get('vipps_order_id', '')),
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = str(''),
                code    = int(200),
                ok      = bool(True)
            )
        )

    """
    rpc UpdatePaymentInfo(PaymentInfo) returns (PaymentInfo) {}
    """
    def UpdatePaymentInfo(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        empty_response = vedbjorn_pb2.PaymentInfo(
            mongodb_id          = str('') ,
            paying_user_email   = str('') ,
            paying_user_phone   = str('') ,
            receiving_user_name = str('') ,
            message_to_payer    = str('') ,
            ref_code            = str('') ,
            ref_collection      = str('') ,
            ref_visit_id        = str('') ,
            ref_route_id        = str('') ,
            status              = str('') ,
            amount_NOK          = float(0.0) ,
            calc_time           = float(0.0) ,
            vipps_order_id      = '',
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = str('Resource unavailable') ,
                code    = int(400) ,
                ok      = bool(False)
            )
        )

        if not self.let_the_client_in(context) :
            empty_response.info.code = 401
            empty_response.info.ok = False
            empty_response.info.content = ''
            return empty_response

        mongodb_id = request.mongodb_id
        if not mongodb_id :
            empty_response.info.content = 'Not found'
            empty_response.info.code = 404
            return empty_response
        db = get_db()
        payment = db.insist_on_find_one('vipps_payments_in', mongodb_id)
        if not payment :
            empty_response.info.content = 'Not found'
            empty_response.info.code = 404
            return empty_response

        order_id = request.vipps_order_id
        if order_id :
            # if the order is updated, it implies that a new vipps payment process has been
            # initiated. Then we also need to update the status to "unpaid" to catch & claim
            db.insist_on_update_one(payment, 'vipps_payments_in', 'vipps_order_id', order_id)
            db.insist_on_update_one(payment, 'vipps_payments_in', 'status', 'unpaid')

        status = request.status
        if status :
            db.insist_on_update_one(payment, 'vipps_payments_in', 'status', status)

        return vedbjorn_pb2.PaymentInfo(
            mongodb_id=str(payment['_id']),
            paying_user_email=str(payment['paying_user']['email']),
            paying_user_phone=str(payment['paying_user']['phone']),
            receiving_user_name=str(payment['receiving_user']),
            message_to_payer=str(payment['message']),
            ref_code=str(payment['ref']['the_code']),
            ref_collection=str(payment['ref']['collection']),
            ref_visit_id=str(payment['ref']['visit_id']),
            ref_route_id=str(payment['ref']['route']),
            status=str(payment['status']),
            amount_NOK=float(payment['amount_NOK']),
            calc_time=float(payment['calc_time']),
            vipps_order_id=str(order_id),
            info=vedbjorn_pb2.GeneralInfoMessage(
                content=str(''),
                code=int(200),
                ok=bool(True)
            )
        )


    """
        rpc UpdateCompany(Company) returns (Company) {}
    """
    def UpdateCompany(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        empty_response = vedbjorn_pb2.Company(
            billname       = str('') ,
            accountnum     = str('') ,
            companyname    = str('') ,
            companynum     = str('') ,
            companyaddress = str('') ,
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = str('Resource unavailable') ,
                code    = int(400) ,
                ok      = bool(False)
            )
        )

        if not self.let_the_client_in(context) :
            empty_response.info.code = 401
            empty_response.info.ok = False
            empty_response.info.content = ''
            return empty_response

        email_address = request.owner.email_address
        phone_number = request.owner.phone_number
        billname = request.billname
        accountnum = request.accountnum
        companyname = request.companyname
        companynum = request.companynum
        companyaddress = request.companyaddress

        db = get_db()
        already_company = db.insist_on_find_one_q('companies', {
            'email_address' : email_address ,
            'phone_number' : phone_number
        })
        if already_company :
            if billname :
                db.insist_on_update_one(already_company, 'companies', 'billname', billname)
            if accountnum :
                db.insist_on_update_one(already_company, 'companies', 'accountnum', accountnum)
            if companyname :
                db.insist_on_update_one(already_company, 'companies', 'companyname', companyname)
            if companynum :
                db.insist_on_update_one(already_company, 'companies', 'companynum', companynum)
            if companyaddress :
                db.insist_on_update_one(already_company, 'companies', 'companyaddress', companyaddress)
        else:

            if companyname and companyname != '_' and companynum and companynum != '_' :
                another_company = db.insist_on_find_one_q('companies' , {
                    '$or' : [
                        {'companyname' : companyname} ,
                        {'companynum'  : companynum }
                    ]
                })
                if another_company :
                    empty_response.info.code = 400
                    empty_response.info.ok = False
                    empty_response.info.content = 'Company-info conflict / already taken.'
                    return empty_response

            db.insist_on_insert_one('companies', {
                'email_address' : email_address ,
                'phone_number' : phone_number ,
                'billname' : billname ,
                'accountnum' : accountnum ,
                'companyname' : companyname ,
                'companynum' : companynum ,
                'companyaddress' : companyaddress
            })

        already_company = db.insist_on_find_one_q('companies', {
            'email_address': email_address,
            'phone_number': phone_number
        })
        if not already_company :
            empty_response.info.code = 500
            empty_response.info.ok = False
            empty_response.info.content = 'Failed to store company information'
            return empty_response
        return vedbjorn_pb2.Company(
            owner = vedbjorn_pb2.UserContactInfo(
                email_address = str(already_company.get('email_address' , '')) ,
                phone_number  = str(already_company.get('phone_number', ''))
            ),
            billname       = str(already_company.get('billname' , '')) ,
            accountnum     = str(already_company.get('accountnum' , '')) ,
            companyname    = str(already_company.get('companyname' , '')) ,
            companynum     = str(already_company.get('companynum' , '')) ,
            companyaddress = str(already_company.get('companyaddress' , '')) ,
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = str('') ,
                code    = int(200) ,
                ok      = bool(True)
            )
        )


    """
        rpc GetCompany(Company) returns (Company) {}
    """

    def GetCompany(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        empty_response = vedbjorn_pb2.Company(
            billname=str(''),
            accountnum=str(''),
            companyname=str(''),
            companynum=str(''),
            companyaddress=str(''),
            info=vedbjorn_pb2.GeneralInfoMessage(
                content=str('Resource unavailable'),
                code=int(400),
                ok=bool(False)
            )
        )

        if not self.let_the_client_in(context) :
            empty_response.info.code = 401
            empty_response.info.ok = False
            empty_response.info.content = ''
            return empty_response

        email_address = request.owner.email_address
        phone_number = request.owner.phone_number
        db = get_db()
        if email_address or phone_number :
            already_company = db.insist_on_find_one_q('companies' , {
                '$or' : [
                    {'email_address' : email_address} ,
                    {'phone_number': phone_number}
                ]
            })
            if already_company :
                return vedbjorn_pb2.Company(
                    owner=vedbjorn_pb2.UserContactInfo(
                        email_address=str(already_company.get('email_address', '')),
                        phone_number=str(already_company.get('phone_number', ''))
                    ),
                    billname=str(already_company.get('billname', '')),
                    accountnum=str(already_company.get('accountnum', '')),
                    companyname=str(already_company.get('companyname', '')),
                    companynum=str(already_company.get('companynum', '')),
                    companyaddress=str(already_company.get('companyaddress', '')),
                    info=vedbjorn_pb2.GeneralInfoMessage(
                        content=str(''),
                        code=int(200),
                        ok=bool(True)
                    )
                )
            else :
                empty_response.info.code = 200
                empty_response.info.ok = True
                empty_response.info.content = 'Not Found'
                return empty_response

        companyname = request.companyname
        companynum = request.companynum
        if not companyname and not companynum :
            empty_response.info.code = 400
            empty_response.info.ok = False
            empty_response.info.content = 'Invalid query'
            return empty_response

        already_company = db.insist_on_find_one_q('companies', {
            '$or': [
                {'companyname': companyname},
                {'companynum': companynum}
            ]
        })
        if already_company:
            return vedbjorn_pb2.Company(
                owner=vedbjorn_pb2.UserContactInfo(
                    email_address=str(already_company.get('email_address', '')),
                    phone_number=str(already_company.get('phone_number', ''))
                ),
                billname=str(already_company.get('billname', '')),
                accountnum=str(already_company.get('accountnum', '')),
                companyname=str(already_company.get('companyname', '')),
                companynum=str(already_company.get('companynum', '')),
                companyaddress=str(already_company.get('companyaddress', '')),
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content=str(''),
                    code=int(200),
                    ok=bool(True)
                )
            )
        else:
            empty_response.info.code = 404
            empty_response.info.ok = False
            empty_response.info.content = 'Not Found'
            return empty_response


    """
        rpc UpdateBatchSellRequest(BatchSellRequest) returns (BatchSellRequest) {}
    """
    def UpdateBatchSellRequest(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        empty_response = vedbjorn_pb2.BatchSellRequest(
            owner=vedbjorn_pb2.UserContactInfo(
                email_address=str(''),
                phone_number=str('')
            ),
            info = vedbjorn_pb2.GeneralInfoMessage(
                content = str('Resource unavailable') ,
                code    = int(400) ,
                ok      = bool(False)
            )
        )

        if not self.let_the_client_in(context) :
            empty_response.info.code = 401
            empty_response.info.ok = False
            empty_response.info.content = ''
            return empty_response

        email_address = request.owner.email_address
        phone_number = request.owner.phone_number

        db = get_db()
        has_sent_batchsell_req = db.insist_on_find_one_q('batchsell_requests' , {
                '$or' : [
                    {'email_address' : email_address} ,
                    {'phone_number': phone_number}
                ]
            })
        if has_sent_batchsell_req :
            empty_response.info.code = 200
            empty_response.info.ok = True
            empty_response.info.content = 'sent'
            return empty_response

        user = get_user_with_email(email_address)
        if not user:
            empty_response.info.content = 'user not found'
            return empty_response
        userObj = user[0][0]

        loc = get_user_location(email_address)
        if not loc:
            empty_response.info.content = 'location not found'
            return empty_response
        locObj = loc[0][0]

        notif_to_buyer = db.insist_on_insert_one('notifications', {
            'email' : 'stian@vedbjorn.no' ,
            'timestamp' : datetime.datetime.utcnow().timestamp() ,
            'contentType' : 'BatchSellRequest' ,
            'text' : 'En bruker ønsker å selge vedlass : \n ' + \
                     'Navn : ' + userObj.get('firstname' , '') + ' ' + userObj.get('lastname' , '') + ' , \n' + \
                     'Tlf : ' + userObj.get('phone' , '') + ' , \n' + \
                     'Epost : ' + userObj.get('email' , '') + ' , \n' + \
                     'Adresse : ' + locObj.get('display_name' , '') ,
            'status' : 'new'
        })

        notif_to_seller = db.insist_on_insert_one('notifications', {
            'email': email_address,
            'timestamp': datetime.datetime.utcnow().timestamp(),
            'contentType': 'BatchSellRequest',
            'text': 'Så fint at du vil selge ved til Vedbjørn! Vi har fått beskjed om dette og vil ta kontakt med deg '
                    'så fort vi kan',
            'status': 'new'
        })

        db.insist_on_insert_one('batchsell_requests' , {
            'email_address' : email_address,
            'phone_number' : phone_number,
            'notification_to_buyer' : notif_to_buyer ,
            'notification_to_seller' : notif_to_seller
        })

        empty_response.info.code = 200
        empty_response.info.ok = True
        empty_response.info.content = 'sent'
        return empty_response

    """
        rpc GetBatchSellRequest(BatchSellRequest) returns (BatchSellRequest) {}
    """

    def GetBatchSellRequest(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        empty_response = vedbjorn_pb2.BatchSellRequest(
            owner=vedbjorn_pb2.UserContactInfo(
                email_address=str(''),
                phone_number=str('')
            ),
            info=vedbjorn_pb2.GeneralInfoMessage(
                content=str('Resource unavailable'),
                code=int(400),
                ok=bool(False)
            )
        )

        if not self.let_the_client_in(context) :
            empty_response.info.code = 401
            empty_response.info.ok = False
            empty_response.info.content = ''
            return empty_response

        email_address = request.owner.email_address
        phone_number = request.owner.phone_number
        db = get_db()
        has_sent_batchsell_req = db.insist_on_find_one_q('batchsell_requests', {
            '$or': [
                {'email_address': email_address},
                {'phone_number': phone_number}
            ]
        })
        if has_sent_batchsell_req:
            empty_response.info.code = 200
            empty_response.info.ok = True
            empty_response.info.content = 'sent'
            return empty_response

        empty_response.info.code = 200
        empty_response.info.ok = True
        empty_response.info.content = 'not sent'
        return empty_response

    def GetMarketInfo(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.MarketInfo(
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=401,
                    ok=False
                )
            )

        if request.county != '' and request.municipality != '' :
            sellers = get_sell_requests_in_muni(request.county, request.municipality)
            buyers = get_buy_requests_in_muni(request.county, request.municipality)
            drivers = get_drive_requests_in_muni(request.county, request.municipality)
            return vedbjorn_pb2.MarketInfo(
                num_sellers = len(sellers) ,
                num_buyers = len(buyers),
                num_drivers = len(drivers) ,
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=200,
                    ok=True
                )
            )
        elif request.county != '' :
            sellers = get_sell_requests_in_county(request.county)
            buyers = get_buy_requests_in_county(request.county)
            drivers = get_drive_requests_in_county(request.county)
            return vedbjorn_pb2.MarketInfo(
                num_sellers=len(sellers),
                num_buyers=len(buyers),
                num_drivers=len(drivers) ,
                info=vedbjorn_pb2.GeneralInfoMessage(
                    content='',
                    code=200,
                    ok=True
                )
            )
        else :
            return vedbjorn_pb2.MarketInfo(
                info = vedbjorn_pb2.GeneralInfoMessage(
                    content = 'Not enough arguments provided to find any market info' ,
                    code = 400 ,
                    ok = False
                )
            )

    """
    rpc OrderAdmMassEmails(AdmMassEmails) returns (GeneralInfoMessage) {}
    """
    def OrderAdmMassEmails(self, request, context):
        """

        :param request:
        :param context:
        :return:
        """

        if not self.let_the_client_in(context) :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=403,
                ok=False)

        title = request.title
        text = request.text
        toBuyers = request.toBuyers
        toSellers = request.toSellers
        toDrivers = request.toDrivers
        emails = request.emails

        counties = get_all_countys()
        uniq_emails: dict = {}

        for county in counties:
            if not county or len(county) <= 0:
                continue
            countyName = county[0].get('name', '')
            if toDrivers :
                drivers_in_county = get_all_drivers_in_county(countyName)
                for graph_ret in drivers_in_county :
                    uniq_emails[graph_ret[1]['email']] = 1
            if toSellers :
                sellers_in_county = get_all_sellers_in_county(countyName)
                for graph_ret in sellers_in_county :
                    uniq_emails[graph_ret[1]['email']] = 1
            if toBuyers :
                buyers_in_county  = get_all_buyers_in_county(countyName)
                for graph_ret in buyers_in_county :
                    uniq_emails[graph_ret[1]['email']] = 1

        for email in emails :
            uniq_emails[email] = 1

        uniq_emails_list = list()
        for email, _ in uniq_emails.items() :
            uniq_emails_list.append(email)

        db = get_db()
        db.insist_on_insert_one('email_orders' , {
            'title' : title ,
            'text' : text ,
            'recipients' : uniq_emails_list ,
            'ordered_utc' : datetime.datetime.utcnow() ,
            'status' : 'ordered'
        })

        return vedbjorn_pb2.GeneralInfoMessage(
            content='',
            code=200,
            ok=True
        )

    """
      rpc GetPrices(Nothing) returns (AllPrices) {}
    """
    def GetPrices(self, request, context):
        """
        :param request:
        :param context:
        :return:
        """
        if not self.let_the_client_in(context):
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=403,
                ok=False)

        db = get_db()
        prices : vedbjorn_pb2.AllPrices = vedbjorn_pb2.AllPrices()
        prices_it = db.insist_on_find('prices')
        num : int = 0
        for price in mpcur(prices_it) :
            prices.prices.append(vedbjorn_pb2.PriceDefinition(
                county = price['county'] ,
                price = price['price'] ,
                product = price['product']
            ))
            num = num + 1
        if num == 0 :
            db.insist_on_insert_one('prices' , {
                'county' : 'Oslo' ,
                'price' : 150 ,
                'product' : '40 L'
            })
            db.insist_on_insert_one('prices' , {
                'county' : 'Rogaland' ,
                'price' : 150 ,
                'product' : '40 L'
            })
            db.insist_on_insert_one('prices' , {
                'county' : 'Møre og Romsdal' ,
                'price' : 150 ,
                'product' : '40 L'
            })
            db.insist_on_insert_one('prices' , {
                'county' : 'Nordland' ,
                'price' : 150 ,
                'product' : '40 L'
            })
            db.insist_on_insert_one('prices' , {
                'county' : 'Viken' ,
                'price' : 150 ,
                'product' : '40 L'
            })
            db.insist_on_insert_one('prices' , {
                'county' : 'Innlandet' ,
                'price' : 150 ,
                'product' : '40 L'
            })
            db.insist_on_insert_one('prices' , {
                'county' : 'Vestfold og Telemark' ,
                'price' : 150 ,
                'product' : '40 L'
            })
            db.insist_on_insert_one('prices' , {
                'county' : 'Agder' ,
                'price' : 150 ,
                'product' : '40 L'
            })
            db.insist_on_insert_one('prices' , {
                'county' : 'Vestland' ,
                'price' : 150 ,
                'product' : '40 L'
            })
            db.insist_on_insert_one('prices' , {
                'county' : 'Trøndelag' ,
                'price' : 150 ,
                'product' : '40 L'
            })
            db.insist_on_insert_one('prices' , {
                'county' : 'Troms og Finnmark' ,
                'price' : 150 ,
                'product' : '40 L'
            })
            prices_it = db.insist_on_find('prices')
            for price in mpcur(prices_it):
                prices.prices.append(vedbjorn_pb2.PriceDefinition(
                    county=price['county'],
                    price=price['price'],
                    product=price['product']
                ))
        return prices

    """
        rpc SetPrices(AllPrices) returns (GeneralInfoMessage) {}
    """
    def SetPrices(self, request, context):
        """
        :param request:
        :param context:
        :return:
        """
        if not self.let_the_client_in(context):
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=403,
                ok=False)

        db = get_db()
        for price in request.prices :
            prev_price = db.insist_on_find_one_q('prices', {
                'county' : price.county ,
                'product' : price.product
            })
            if prev_price :
                db.insist_on_update_one(prev_price, 'prices', 'price', price.price)
            else:
                db.insist_on_insert_one('prices', {
                    'county': price.county,
                    'price' : price.price,
                    'product': price.product
                })

        return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=200,
                ok=True)

    """
    rpc SetSeasonOnOrOff(Name) returns (GeneralInfoMessage) {}
    rpc GetSeasonOnOrOff(Nothing) returns (Name) {}
    """
    def SetSeasonOnOrOff(self, request, context):
        """
        :param request:
        :param context:
        :return:
        """
        if not self.let_the_client_in(context):
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=403,
                ok=False)

        on_or_off = request.value
        if on_or_off != 'on' and on_or_off != 'off' :
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=400,
                ok=False)

        db = get_db()
        seasonInfo = db.insist_on_find_most_recent('season')
        if seasonInfo :
            db.insist_on_update_one(seasonInfo, 'season', 'status', on_or_off)
        else:
            db.insist_on_insert_one('season', {'status' : on_or_off})
        return vedbjorn_pb2.GeneralInfoMessage(
            content='',
            code=200,
            ok=True)

    def GetSeasonOnOrOff(self, request, context):
        """
        :param request:
        :param context:
        :return:
        """
        print('GetSeasonOnOrOff - BEGIN -')
        if not self.let_the_client_in(context):
            return vedbjorn_pb2.GeneralInfoMessage(
                content='',
                code=403,
                ok=False)

        db = get_db()
        seasonInfo = db.insist_on_find_most_recent('season')
        if seasonInfo:
            return vedbjorn_pb2.Name(value = seasonInfo['status'])
        else:
            db.insist_on_insert_one('season', {'status': 'on'})
            return vedbjorn_pb2.Name(value='on')

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    vedbjorn_pb2_grpc.add_VedbjornFunctionsServicer_to_server(VedbjornServer(), server)

    if DEBUGGING :
        with open('./certificate.pem', 'rb') as f:
            cert = f.read()
        with open('./key.pem', 'rb') as f:
            key = f.read()
        server_credentials = grpc.ssl_server_credentials(((key, cert),))
        server.add_secure_port('[::]:443', server_credentials)
    else :
        """
        The Google Cloudrun provides a SSL/TLS proxy
        """
        server.add_insecure_port('[::]:443')

    server.start()
    print('gRPC-server started')
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig()
    serve()

    print('Vedbjørn gRPC finished')