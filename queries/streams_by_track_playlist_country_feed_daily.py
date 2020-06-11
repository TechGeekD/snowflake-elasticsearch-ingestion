def formatQuery(offset, limit, start_dateStr, end_dateStr):
    query = f"""
            WITH top_sound AS (
                SELECT stp.*, pf.followers
                FROM STREAMS_BY_TRACK_PLAYLIST_COUNTRY_FEED_DAILY AS stp
                LEFT JOIN MAPPINGS_DIM_PLAYLIST_TO_FOLLOWERS AS pf USING(stp.playlist_id)
                WHERE stp.DOWNLOAD_ACTIVITY_DATE between '{start_dateStr}' and '{end_dateStr}'
                LIMIT {limit}
                OFFSET {offset}
            )
            SELECT
                OBJECT_CONSTRUCT(
                    'isrc', ts.isrc,
                    'feed_id',ts.feed_id,
                    'store_id',ts.store_id,
                    'label_id',ts.label_id,
                    'track_id',ts.track_id,
                    'followers',ts.followers,
                    'product_id',ts.product_id,
                    'country_code',ts.country_code,
                    'playlist_id',ts.playlist_id,
                    'download_activity_date',ts.download_activity_date,
                    'store_playlist_id',ts.store_playlist_id,
                    'playlist_name',ts.playlist_name,
                    'playlist_url',ts.playlist_url,
                    'playlist_image',ts.playlist_image,
                    'streams',ts.streams
                )
            FROM top_sound as ts
        """

    return query