package edu.utexas.clm.archipelago.ijsupport.bottle.tile;

import edu.utexas.clm.archipelago.network.MessageXC;
import edu.utexas.clm.archipelago.network.translation.Bottle;
import ini.trakem2.display.Patch;
import mpicbg.models.TranslationModel2D;
import mpicbg.trakem2.align.AbstractAffineTile2D;
import mpicbg.trakem2.align.TranslationTile2D;

import java.io.IOException;

/**
 *
 */
public class TranslationTile2DBottle implements Bottle<AbstractAffineTile2D<?>>
{
    private final TranslationModel2D model;
    private final Patch patch;

    public TranslationTile2DBottle(final TranslationTile2D tile)
    {
        model = tile.getModel();
        patch = tile.getPatch();
    }

    public AbstractAffineTile2D<?> unBottle(final MessageXC xc) throws IOException
    {
        return new TranslationTile2D(model, patch);
    }
}