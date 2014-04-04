package edu.utexas.clm.archipelago.ijsupport.bottle.tile;

import edu.utexas.clm.archipelago.ijsupport.bottle.TileBottle;
import edu.utexas.clm.archipelago.network.MessageXC;
import ini.trakem2.display.Patch;
import mpicbg.models.SimilarityModel2D;
import mpicbg.trakem2.align.AbstractAffineTile2D;
import mpicbg.trakem2.align.SimilarityTile2D;

import java.io.IOException;

/**
 *
 */
public class SimilarityTile2DBottle extends TileBottle
{
    private static class IDSimilarityTile2D extends SimilarityTile2D
    {
        private final long id;

        public IDSimilarityTile2D(final SimilarityModel2D model, final Patch patch, long id)
        {
            super(model, patch);
            this.id = id;
        }
    }

    private final SimilarityModel2D model;

    public SimilarityTile2DBottle(final SimilarityTile2D tile, boolean isOrigin)
    {
        super(isOrigin, tile.getPatch());

        model = tile.getModel();

        if (isOrigin)
        {
            id = getNextId();
            placeTile(id, tile);
        }
        else
        {
            if (tile instanceof IDSimilarityTile2D)
            {
                id = ((IDSimilarityTile2D)tile).id;
            }
            else
            {
                id = -1;
            }
        }
    }

    public AbstractAffineTile2D<?> unBottle(final MessageXC xc) throws IOException
    {
        if (fromOrigin)
        {
            return new IDSimilarityTile2D(model, patch, id);
        }
        else
        {
            if (id > 0)
            {
                return getTile(id);
            }
            else
            {
                return new SimilarityTile2D(model, patch);
            }
        }
    }
}
