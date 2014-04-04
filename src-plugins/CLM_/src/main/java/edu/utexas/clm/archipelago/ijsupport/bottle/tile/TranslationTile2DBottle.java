package edu.utexas.clm.archipelago.ijsupport.bottle.tile;

import edu.utexas.clm.archipelago.ijsupport.bottle.TileBottle;
import edu.utexas.clm.archipelago.network.MessageXC;
import ini.trakem2.display.Patch;
import mpicbg.models.TranslationModel2D;
import mpicbg.trakem2.align.AbstractAffineTile2D;
import mpicbg.trakem2.align.TranslationTile2D;

import java.io.IOException;

/**
 *
 */
public class TranslationTile2DBottle extends TileBottle
{
    private static class IDTranslationTile2D extends TranslationTile2D
    {
        private final long id;

        public IDTranslationTile2D(final TranslationModel2D model, final Patch patch, long id)
        {
            super(model, patch);
            this.id = id;
        }
    }

    private final TranslationModel2D model;

    public TranslationTile2DBottle(final TranslationTile2D tile, boolean isOrigin)
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
            if (tile instanceof IDTranslationTile2D)
            {
                id = ((IDTranslationTile2D)tile).id;
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
            return new IDTranslationTile2D(model, patch, id);
        }
        else
        {
            if (id > 0)
            {
                return getTile(id);
            }
            else
            {
                return new TranslationTile2D(model, patch);
            }
        }
    }
}
